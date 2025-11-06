// Gap Products Scraper
// Visits Gap website and API endpoint to parse and save JSON data
//
// RESUME FUNCTIONALITY:
// - First run: Fetches all products from Gap API and saves to products.json
// - Subsequent runs: Loads from products.json and processes only unprocessed products
// - Products are processed one by one for detailed variants (colors/sizes)
// - Each processed product is saved to processed.json immediately
// - Script can be paused and resumed - progress is saved after each product
// - At the end, products are validated and catalog files are generated
//
// FILE STRUCTURE:
// - products.json: All products from API with processing status
// - processed.json: Successfully processed products with full variant details
// - catalog.json/jsonl/gz: Final validated catalog files
//
// VALIDATION:
// - Uses filterValidProducts from validate-catalog.js
// - Validates mandatory fields for products and variants
// - Filters out invalid products and variants
// - Validation results included in catalog store_info
require('dotenv').config()
const fs = require('fs')
const path = require('path')
const puppeteer = require('puppeteer-extra')
const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')

// Add stealth plugin and use defaults (all evasion techniques)
const StealthPlugin = require('puppeteer-extra-plugin-stealth')
puppeteer.use(StealthPlugin())

// Import helper functions and database
const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const Store = require('./models/Store')
const {
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  cleanAndTruncate,
  getDomainName,
} = require('./utils/helper')
const { filterValidProducts } = require('./validate-catalog')

// Configuration
const GAP_WEBSITE_URL =
  'https://www.gap.com/browse/women/shop-all-styles?cid=1127938&nav=meganav%3AWomen%3ACategories%3AShop%20All%20Styles'
const GAP_API_BASE_URL = 'https://api.gap.com/commerce/search/products/v2/cc'

// Create output directory if it doesn't exist
const outputDir = path.join(__dirname, 'output')
if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true })
}

// Helper function to chunk array into smaller arrays
const chunkArray = (array, chunkSize) => {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

// Helper function to scrape product detail page for colors and sizes
const scrapeProductDetails = async (page, productUrl, productId) => {
  try {
    console.log(`  🔍 Navigating to product detail page: ${productUrl}`)

    // Navigate to product detail page with retry logic
    let navigationSuccess = false
    let navRetries = 0
    const maxNavRetries = 3

    while (navRetries < maxNavRetries && !navigationSuccess) {
      try {
        await page.goto(productUrl, {
          waitUntil: 'domcontentloaded',
          timeout: 30000,
        })
        navigationSuccess = true
      } catch (navError) {
        navRetries++
        console.log(
          `    ⚠️ Navigation failed (attempt ${navRetries}): ${navError.message}`
        )

        if (navRetries < maxNavRetries) {
          console.log('    ⏳ Waiting 2 seconds before retry...')
          await new Promise((resolve) => setTimeout(resolve, 2000))
        } else {
          throw navError
        }
      }
    }

    // Wait a bit for the page to load completely
    await new Promise((resolve) => setTimeout(resolve, 2000))

    await page.waitForSelector('.pdp-color-picker-group-container__swatches')

    // Extract product description
    let description = ''
    try {
      description = await page.evaluate(() => {
        // Try multiple selectors for the description
        const descriptionElement = document.querySelector(
          'body > main > div > div:nth-child(3) > div:nth-child(1) > div > div > div:nth-child(2) > div:nth-child(3) > div > div:nth-child(7) > div > div:nth-child(1) > div > div > div > p'
        )
        if (descriptionElement) {
          return descriptionElement.textContent?.trim() || ''
        }

        // Fallback selectors
        const altElement = document.querySelector(
          '.pdp-description p, .product-description p, [class*="description"] p'
        )
        return altElement?.textContent?.trim() || ''
      })
      console.log(
        `    📝 Description extracted: ${description.substring(0, 100)}...`
      )
    } catch (error) {
      console.log(`    ⚠️ Could not extract description: ${error.message}`)
    }

    // Extract colors and their sizes
    const colors = []

    // Get all color elements
    const colorElements = await page.$$('.pdp-color-picker-swatch-container')
    console.log(`    Found ${colorElements.length} color elements`)

    for (let i = 0; i < colorElements.length; i++) {
      try {
        const colorElement = colorElements[i]

        // Get color information
        const colorId = await colorElement.evaluate((el) =>
          el.getAttribute('id')
        )
        const colorName = await colorElement.evaluate(
          (el) =>
            el.getAttribute('aria-label') ||
            el.getAttribute('title') ||
            el.querySelector('img')?.getAttribute('alt') ||
            `Color ${i + 1}`
        )

        console.log(`    Processing color ${i + 1}: ${colorName}`)

        // Click on the color to load its sizes
        await colorElement.click()

        // Wait for sizes to load
        try {
          await page.waitForSelector('.pdp_size-selector-container__items', {
            timeout: 5000,
          })
        } catch (waitError) {
          console.log(`    ⚠️ Size selector not found for color ${colorName}`)
          continue
        }

        // Extract sizes for this color
        const sizes = await page.evaluate(() => {
          const sizeParentElement = document.querySelector(
            '.pdp_size-selector-container__items'
          )
          const extractedSizes = []

          if (sizeParentElement) {
            sizeParentElement
              .querySelectorAll('.fds_selector__label')
              .forEach((sizeElement) => {
                // Skip unavailable sizes
                if (
                  sizeElement.classList.contains(
                    'fds_selector__label--unavailable'
                  )
                ) {
                  return
                }

                const sizeName =
                  sizeElement.textContent?.trim() ||
                  sizeElement.getAttribute('aria-label') ||
                  sizeElement.getAttribute('title') ||
                  'Unknown Size'

                const isAvailable =
                  !sizeElement.classList.contains(
                    'fds_selector__label--unavailable'
                  ) &&
                  !sizeElement.classList.contains('disabled') &&
                  !sizeElement.hasAttribute('disabled')

                if (sizeName && sizeName !== 'Unknown Size') {
                  extractedSizes.push({
                    name: sizeName,
                    isAvailable: isAvailable,
                  })
                }
              })
          }

          return extractedSizes
        })

        console.log(
          `    Found ${sizes.length} available sizes for ${colorName}:`,
          sizes.map((s) => s.name)
        )

        colors.push({
          name: colorName.trim(),
          colorId: colorId,
          isAvailable: true, // If we can click it, it's available
          sizes: sizes,
        })

        // Small delay between color clicks
        await new Promise((resolve) => setTimeout(resolve, 1500))
      } catch (error) {
        console.log(`    ⚠️ Error extracting color ${i + 1}:`, error.message)
      }
    }

    console.log(`    ✅ Found ${colors.length} colors with sizes`)

    return {
      colors: colors,
      description: description,
    }
  } catch (error) {
    console.log(
      `    ❌ Error scraping product details for ${productId}:`,
      error.message
    )
    return {
      colors: [],
      sizes: [],
      productImages: [],
      description: '',
    }
  }
}

// Helper function to process a single product
const processProduct = async (
  product,
  gender = 'Women',
  category = '',
  page = null
) => {
  console.log(`Processing product: ${product.styleName}`)

  const productId = product.styleId?.toString()
  const productUrl = `https://www.gap.com/browse/product.do?pid=${product.styleId}#pdp-page-content`

  // Scrape product detail page for colors, sizes, and description if page is provided
  let detailPageData = {
    colors: [],
    sizes: [],
    productImages: [],
    description: '',
  }
  if (page) {
    detailPageData = await scrapeProductDetails(page, productUrl, productId)
  }

  if (!detailPageData || detailPageData.colors.length === 0) return

  console.log(
    `Found ${detailPageData.colors.length} colors from detail page scraping`
  )

  const formattedProduct = {
    parent_product_id: productId,
    name: product.styleName,
    description: detailPageData.description || '',
    category: category || product.webProductType || '',
    retailer_domain: 'gap.com',
    brand: 'Gap',
    gender: gender,
    materials: '',
    return_policy_link:
      'https://www.gap.com/customer-service/how-to-return-exchange-items?cid=81264',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'gap',
  }

  // Process variants using scraped data from detail page
  if (product.styleColors && product.styleColors.length > 0) {
    for (const color of product.styleColors) {
      const scrapedColor = detailPageData.colors.find(
        (c) => c.colorId === color.ccId
      )

      if (!scrapedColor) continue

      const colorName = `${color.ccShortDescription}`
      const originalPrice = parseFloat(color.regularPrice) || 0
      const sellingPrice = parseFloat(color.effectivePrice) || 0
      const finalPrice = sellingPrice > 0 ? sellingPrice : originalPrice
      const discount = calculateDiscount(originalPrice, finalPrice)
      const isOnSale = discount > 0

      // Get images for this color
      let imageUrl = ''
      let alternateImages = []

      if (color.images && color.images.length > 0) {
        // VLI,AV6_Z,AV6_VLI,AV6_PRST,AV3_PRST,AV1_PRST,AV2_PRST,AV4_PRST,AV5_PRST,P01_PRST
        // Get the first image as main image (prefer AV1 type)
        const mainImage = color.images.find((img) =>
          img.type.includes(
            'VLI,AV6_Z,AV6_VLI,AV6_PRST,AV3_PRST,AV1_PRST,AV2_PRST,AV4_PRST,AV5_PRST,P01_PRST'
          )
        )
        if (mainImage && mainImage.path) {
          imageUrl = `https://www.gap.com${mainImage.path}`
        }

        // Get alternate images (up to 5)
        alternateImages = color.images
          .filter((img) =>
            img.type.includes(
              'VLI,AV6_Z,AV6_VLI,AV6_PRST,AV3_PRST,AV1_PRST,AV2_PRST,AV4_PRST,AV5_PRST,P01_PRST'
            )
          )
          .map((img) => (img.path ? `https://www.gap.com${img.path}` : ''))
          .filter((url) => url)
      }

      // Create variants for each size of this color
      if (scrapedColor.sizes && scrapedColor.sizes.length > 0) {
        for (const size of scrapedColor.sizes) {
          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url: productUrl,
            deeplink_url: productUrl,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: true,
            size: size.name,
            color: colorName,
            mpn: uuidv5(
              `${product.styleId}-${colorName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: product.reviewCount || 0,
            average_ratings: product.reviewScore || 0,
            review_count: product.reviewCount || 0,
            selling_price: sellingPrice,
            sale_price: isOnSale ? sellingPrice : null,
            final_price: finalPrice,
            discount: discount,
            operation_type: 'INSERT',
            variant_id: uuidv5(
              `${productId}-${color.ccId}-${size.name}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
            ),
            variant_description: '',
          }
          formattedProduct.variants.push(formattedVariant)
        }
      } else {
        // If no sizes found, create a variant without size
        const formattedVariant = {
          price_currency: 'USD',
          original_price: originalPrice,
          link_url: productUrl,
          deeplink_url: productUrl,
          image_url: imageUrl,
          alternate_image_urls: alternateImages,
          is_on_sale: isOnSale,
          is_in_stock: scrapedColor.isAvailable,
          size: '',
          color: scrapedColor.name,
          mpn: uuidv5(
            `${product.styleId}-${scrapedColor.name}`,
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
          ),
          ratings_count: product.reviewCount || 0,
          average_ratings: product.reviewScore || 0,
          review_count: product.reviewCount || 0,
          selling_price: sellingPrice,
          sale_price: isOnSale ? sellingPrice : null,
          final_price: finalPrice,
          discount: discount,
          operation_type: 'INSERT',
          variant_id: uuidv5(
            `${productId}-${scrapedColor.name}`,
            '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
          ),
          variant_description: '',
        }
        formattedProduct.variants.push(formattedVariant)
      }
    }
  } else {
    // Fallback to API data if scraping failed
    console.log('  ⚠️ No colors found from scraping, falling back to API data')
    if (product.styleColors && product.styleColors.length > 0) {
      for (const color of product.styleColors) {
        const colorName = `${color.ccName} ${
          color.ccShortDescription || ''
        }`.trim()
        const originalPrice = parseFloat(color.regularPrice) || 0
        const sellingPrice = parseFloat(color.effectivePrice) || 0
        const finalPrice = sellingPrice > 0 ? sellingPrice : originalPrice
        const discount = calculateDiscount(originalPrice, finalPrice)
        const isOnSale = discount > 0

        // Get images for this color
        let imageUrl = ''
        let alternateImages = []

        if (color.images && color.images.length > 0) {
          const mainImage =
            color.images.find((img) => img.type === 'AV1') || color.images[0]
          if (mainImage && mainImage.path) {
            imageUrl = `https://www.gap.com${mainImage.path}`
          }

          alternateImages = color.images
            .slice(1, 6)
            .map((img) => (img.path ? `https://www.gap.com${img.path}` : ''))
            .filter((url) => url)
        }

        const isInStock = color.inventoryStatus === 'In Stock'

        const formattedVariant = {
          price_currency: 'USD',
          original_price: originalPrice,
          link_url: productUrl,
          deeplink_url: productUrl,
          image_url: imageUrl,
          alternate_image_urls: alternateImages,
          is_on_sale: isOnSale,
          is_in_stock: isInStock,
          size: '',
          color: colorName,
          mpn: uuidv5(
            `${product.styleId}-${colorName}`,
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
          ),
          ratings_count: product.reviewCount || 0,
          average_ratings: product.reviewScore || 0,
          review_count: product.reviewCount || 0,
          selling_price: sellingPrice,
          sale_price: isOnSale ? sellingPrice : null,
          final_price: finalPrice,
          discount: discount,
          operation_type: 'INSERT',
          variant_id: uuidv5(
            `${productId}-${color.ccId}-${colorName}`,
            '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
          ),
          variant_description: '',
        }
        formattedProduct.variants.push(formattedVariant)
      }
    }
  }

  // const mongoResult = await saveProductToMongoDB(formattedProduct)

  return { formattedProduct, mongoResult: {} }
}

// Save product to MongoDB
async function saveProductToMongoDB(productData) {
  try {
    // Create new product with INSERT operation type
    productData.operation_type = 'INSERT'
    productData.variants.forEach((variant) => {
      variant.operation_type = 'INSERT'
    })

    const newProduct = new Product(productData)
    await newProduct.save()
    console.log(`✅ Saved to MongoDB: ${productData.name}`)
    return { operation: 'INSERT', product: newProduct }
  } catch (error) {
    console.error(
      `❌ Error saving product ${productData.name} to MongoDB:`,
      error.message
    )
    return { operation: 'ERROR', error: error.message }
  }
}

// Save or update store entry with product IDs
async function saveStoreEntry(storeData, productIds) {
  try {
    // Check if store already exists
    let existingStore = await Store.findOne({
      storeType: 'gap',
      name: 'Gap',
      country: storeData.country || 'US',
    })

    if (existingStore) {
      console.log('Store already exists, updating with new products...')
      // Add new product IDs to existing store (avoid duplicates)
      const existingProductIds = existingStore.products.map((id) =>
        id.toString()
      )
      const newProductIds = productIds.filter(
        (id) => !existingProductIds.includes(id.toString())
      )

      existingStore.products.push(...newProductIds)
      existingStore.isScrapped = true
      existingStore.updatedAt = new Date()

      await existingStore.save()
      console.log(`✅ Updated store with ${newProductIds.length} new products`)
      return { operation: 'UPDATED', store: existingStore }
    } else {
      // Create new store entry
      const newStore = new Store({
        products: productIds,
        name: storeData.name || 'Gap',
        storeTemplate: 'gap-template',
        storeType: 'gap',
        storeUrl: 'https://www.gap.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy:
          'https://www.gap.com/customer-service/how-to-return-exchange-items?cid=81264',
        tags: ['women', 'fashion', 'clothing'],
      })

      await newStore.save()
      console.log(`✅ Created new store with ${productIds.length} products`)
      return { operation: 'CREATED', store: newStore }
    }
  } catch (error) {
    console.error('❌ Error saving store entry:', error.message)
    return { operation: 'ERROR', error: error.message }
  }
}

// Helper function to scrape products from a specific Gap category
async function scrapeGapCategory(
  page,
  categoryConfig,
  targetProductCount = 1500
) {
  let currentPage = 0
  let allProducts = []
  let isLastPage = false

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (!isLastPage && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${currentPage}...`)

    // Build API URL with pagination and category ID
    const apiUrl = `${GAP_API_BASE_URL}?pageSize=50&pageNumber=${currentPage}&ignoreInventory=false&cid=${categoryConfig.cid}&vendor=constructorio&includeMarketingFlagsDetails=true&brand=gap&locale=en_US&market=us`

    try {
      // Navigate to the API URL with retry logic
      let navigationSuccess = false
      let navRetries = 0
      const maxNavRetries = 3

      while (navRetries < maxNavRetries && !navigationSuccess) {
        try {
          console.log(
            `Navigating to API (attempt ${navRetries + 1}/${maxNavRetries})...`
          )
          await page.goto(apiUrl, {
            waitUntil: 'domcontentloaded',
            timeout: 45000,
          })
          navigationSuccess = true
        } catch (navError) {
          navRetries++
          console.log(
            `Navigation failed (attempt ${navRetries}): ${navError.message}`
          )

          if (navRetries < maxNavRetries) {
            console.log('⏳ Waiting 3 seconds before retry...')
            await new Promise((resolve) => setTimeout(resolve, 3000))
          } else {
            throw navError
          }
        }
      }

      console.log(
        `Extracting content from ${categoryConfig.name} page ${currentPage}...`
      )

      // Get the JSON data from the page
      const bodyText = await page.evaluate(() => {
        return document.body.innerText
      })

      // Try to parse if it's JSON
      let pageData = null
      let pageProducts = []

      try {
        pageData = JSON.parse(bodyText)

        // Check pagination info
        if (pageData.pagination) {
          const totalPages = parseInt(pageData.pagination.pageNumberTotal) || 0
          const currentPageNum = parseInt(pageData.pagination.currentPage) || 0
          isLastPage = currentPageNum >= totalPages - 1
          console.log(
            `${categoryConfig.name} Page ${currentPage}: currentPage = ${currentPageNum}, totalPages = ${totalPages}, isLastPage = ${isLastPage}`
          )
        }

        // Extract products from this page
        if (pageData.products && pageData.products.length > 0) {
          pageProducts = pageData.products

          console.log(
            `Found ${pageProducts.length} products on ${categoryConfig.name} page ${currentPage}`
          )
          allProducts = allProducts.concat(pageProducts)
          console.log(
            `${categoryConfig.name} total products so far: ${allProducts.length}`
          )
        } else {
          console.log(
            `No products found on ${categoryConfig.name} page ${currentPage}`
          )
          isLastPage = true // Stop if we can't parse the data
        }

        console.log(
          `Successfully parsed JSON data for ${categoryConfig.name} page ${currentPage}`
        )
      } catch (e) {
        console.log(
          `Error parsing JSON for ${categoryConfig.name} page ${currentPage}:`,
          e.message
        )
        isLastPage = true // Stop if we can't parse the data
      }
    } catch (error) {
      console.error(
        `Error fetching ${categoryConfig.name} page ${currentPage}:`,
        error.message
      )
      isLastPage = true
    }

    currentPage++

    // Add a small delay between requests
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  // Limit to target product count
  if (allProducts.length > targetProductCount) {
    allProducts = allProducts.slice(0, targetProductCount)
  }

  console.log(
    `\n✅ ${categoryConfig.name} pagination completed! Fetched ${
      currentPage - 1
    } pages total.`
  )
  console.log(
    `📦 ${categoryConfig.name} total products collected: ${allProducts.length}`
  )

  return allProducts
}

// Save all products to a JSON file first
async function saveProductsToFile(products, storeData) {
  const countryCode = storeData.country || 'US'
  const cleanBrandName = 'gap'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  const productsFilePath = path.join(dirPath, 'products.json')
  const productsData = {
    store_info: {
      name: storeData.name || 'Gap',
      domain: 'gap.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: products.length,
      categories: ['Women', 'Men', 'Kids', 'Baby'],
      crawled_at: new Date().toISOString(),
    },
    products: products.map((product) => ({
      ...product,
      processed: false, // Track if product details have been processed
      variants: [], // Initialize empty variants array
    })),
  }

  fs.writeFileSync(
    productsFilePath,
    JSON.stringify(productsData, null, 2),
    'utf8'
  )
  console.log(`📄 Products saved to: ${productsFilePath}`)
  return productsFilePath
}

// Load products from file if it exists
async function loadProductsFromFile(storeData) {
  const countryCode = storeData.country || 'US'
  const cleanBrandName = 'gap'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )
  const productsFilePath = path.join(dirPath, 'products.json')

  if (fs.existsSync(productsFilePath)) {
    console.log(`📄 Loading existing products from: ${productsFilePath}`)
    const data = JSON.parse(fs.readFileSync(productsFilePath, 'utf8'))
    return { data, filePath: productsFilePath }
  }
  return null
}

// Update a single product in the products file
async function updateProductInFile(filePath, productIndex, updatedProduct) {
  const data = JSON.parse(fs.readFileSync(filePath, 'utf8'))
  data.products[productIndex] = updatedProduct
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8')
}

// Save a processed product to processed.json file
async function saveProcessedProduct(storeData, formattedProduct) {
  const countryCode = storeData.country || 'US'
  const cleanBrandName = 'gap'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  const processedFilePath = path.join(dirPath, 'processed.json')
  let processedData = { products: [] }

  // Load existing processed data if file exists
  if (fs.existsSync(processedFilePath)) {
    processedData = JSON.parse(fs.readFileSync(processedFilePath, 'utf8'))
  }

  // Add the new processed product
  processedData.products.push(formattedProduct)

  // Save back to file
  fs.writeFileSync(
    processedFilePath,
    JSON.stringify(processedData, null, 2),
    'utf8'
  )
}

// Load all processed products from processed.json
async function loadProcessedProducts(storeData) {
  const countryCode = storeData.country || 'US'
  const cleanBrandName = 'gap'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )
  const processedFilePath = path.join(dirPath, 'processed.json')

  if (fs.existsSync(processedFilePath)) {
    console.log(`📄 Loading processed products from: ${processedFilePath}`)
    const data = JSON.parse(fs.readFileSync(processedFilePath, 'utf8'))
    return data.products || []
  }
  return []
}

// Process products and save to processed.json
async function processProductsToFile(
  products,
  storeData,
  page,
  productsFilePath = null
) {
  const productIds = [] // Track product IDs for store entry
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  }

  // Count how many products need processing
  const unprocessedProducts = products.filter(
    (p) => !p.variants || p.variants.length === 0
  )
  const alreadyProcessed = products.length - unprocessedProducts.length

  // Process products sequentially to avoid overwhelming the browser
  console.log(
    `\n📦 Processing ${products.length} products from all categories sequentially...`
  )
  console.log(`✅ Already processed: ${alreadyProcessed} products`)
  console.log(`⏳ Need to process: ${unprocessedProducts.length} products`)

  for (let i = 0; i < products.length; i++) {
    const product = products[i]
    const gender = product._gender || 'Women'
    const category = product._category || 'Unknown'

    // Skip if product already has variants (already processed)
    if (product.variants && product.variants.length > 0) {
      console.log(
        `⏭️ Skipping ${category} product ${i + 1}/${products.length}: ${
          product.styleName
        } (already processed)`
      )
      continue
    }

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.styleName
      }`
    )

    try {
      const result = await processProduct(product, gender, category, page)

      if (result.formattedProduct) {
        // Save processed product to processed.json
        await saveProcessedProduct(storeData, result.formattedProduct)
        console.log(`💾 Saved processed product to processed.json`)

        // Update the product in the products file with processed variants
        if (productsFilePath) {
          const updatedProduct = {
            ...product,
            variants: result.formattedProduct.variants,
          }
          await updateProductInFile(productsFilePath, i, updatedProduct)
          console.log(`💾 Updated product ${i + 1} in products file`)
        }

        // Track product ID for store entry
        if (result.mongoResult.product) {
          productIds.push(result.mongoResult.product._id)
        }

        if (result.mongoResult.operation === 'INSERT') {
          mongoResults.inserted++
        } else if (result.mongoResult.operation === 'SKIPPED') {
          mongoResults.skipped++
        } else {
          mongoResults.errors++
        }
      }

      // Add a small delay between products to be respectful
      await new Promise((resolve) => setTimeout(resolve, 500))
    } catch (error) {
      console.error(
        `Error processing product ${product.styleName}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  return { mongoResults, productIds }
}

// Generate catalog files from processed.json
async function generateCatalogFiles(storeData) {
  const countryCode = storeData.country || 'US'
  const cleanBrandName = 'gap'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )

  // Load all processed products
  console.log(`\n📚 Loading processed products to generate catalog files...`)
  const processedProducts = await loadProcessedProducts(storeData)
  console.log(`📦 Loaded ${processedProducts.length} processed products`)

  if (processedProducts.length === 0) {
    console.log('⚠️ No processed products found to generate catalog')
    return null
  }

  // Validate products using filterValidProducts
  console.log(`\n🔍 Validating products...`)
  const validationResult = filterValidProducts(processedProducts)

  console.log(`\n📊 Validation Results:`)
  console.log(`  Total products: ${validationResult.totalCount}`)
  console.log(`  Valid products: ${validationResult.validCount}`)
  console.log(`  Invalid products: ${validationResult.invalidCount}`)
  console.log(
    `  Variants filtered: ${validationResult.totalVariantsFiltered || 0}`
  )

  const formattedProducts = validationResult.validProducts

  if (formattedProducts.length === 0) {
    console.log('⚠️ No valid products after validation')
    return null
  }

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'Gap',
      domain: 'gap.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ['Women', 'Men', 'Kids', 'Baby'],
      crawled_at: new Date().toISOString(),
      validation: {
        total_processed: validationResult.totalCount,
        valid_products: validationResult.validCount,
        invalid_products: validationResult.invalidCount,
        variants_filtered: validationResult.totalVariantsFiltered || 0,
      },
    },
    products: formattedProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`✅ JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
  const jsonlContent = formattedProducts
    .map((product) => JSON.stringify(product))
    .join('\n')
  fs.writeFileSync(jsonlFilePath, jsonlContent, 'utf8')
  console.log(`✅ JSONL file generated: ${jsonlFilePath}`)

  // Gzip the JSONL file
  const gzippedFilePath = `${jsonlFilePath}.gz`
  const jsonlBuffer = fs.readFileSync(jsonlFilePath)
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
  fs.writeFileSync(gzippedFilePath, gzippedBuffer)
  console.log(`✅ Gzipped JSONL file generated: ${gzippedFilePath}`)

  return {
    jsonPath: gzippedFilePath,
    totalProducts: formattedProducts.length,
    validationResult: validationResult,
  }
}

async function scrapeGapData() {
  let browser

  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Launching browser for Gap scraping...')
    browser = await puppeteer.launch({
      headless: true,
      defaultViewport: null,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-zygote',
        '--disable-gpu',
        '--disable-background-timer-throttling',
        '--disable-backgrounding-occluded-windows',
        '--disable-renderer-backgrounding',
        '--disable-features=TranslateUI',
        '--disable-ipc-flooding-protection',
        '--window-size=1920,1080',
        '--disable-http2',
        '--disable-features=VizDisplayCompositor',
        '--disable-web-security',
        '--disable-features=site-per-process',
        '--ignore-certificate-errors',
        '--ignore-ssl-errors',
        '--ignore-certificate-errors-spki-list',
        '--disable-extensions',
      ],
    })

    const page = await browser.newPage()

    // Set viewport
    await page.setViewport({ width: 1920, height: 1080 })

    // Set user agent to avoid being blocked
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    // Set extra headers
    await page.setExtraHTTPHeaders({
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate',
      Accept:
        'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
      'Cache-Control': 'no-cache',
      Pragma: 'no-cache',
      'Upgrade-Insecure-Requests': '1',
    })

    // Set default navigation timeout
    page.setDefaultNavigationTimeout(60000)
    page.setDefaultTimeout(60000)

    // Handle page errors
    page.on('error', (error) => {
      console.log('Page error:', error.message)
    })

    page.on('pageerror', (error) => {
      console.log('Page script error:', error.message)
    })

    console.log('📱 Browser launched successfully')

    // Step 1: Visit the Gap website first with retry logic
    console.log('🌐 Visiting Gap website...')
    const maxRetries = 3
    let retryCount = 0
    let websiteLoaded = false

    while (retryCount < maxRetries && !websiteLoaded) {
      try {
        console.log(
          `Attempt ${retryCount + 1}/${maxRetries} to load Gap website...`
        )
        await page.goto(GAP_WEBSITE_URL, {
          waitUntil: 'domcontentloaded',
          timeout: 60000,
        })
        websiteLoaded = true
        console.log('✅ Successfully loaded Gap website')
      } catch (error) {
        retryCount++
        console.log(
          `❌ Failed to load Gap website (attempt ${retryCount}): ${error.message}`
        )

        if (retryCount < maxRetries) {
          console.log(`⏳ Waiting 5 seconds before retry...`)
          await new Promise((resolve) => setTimeout(resolve, 5000))
        } else {
          console.log(
            '⚠️ Skipping initial website visit and proceeding directly to API endpoints...'
          )
          // We'll proceed without the initial website visit since we're using API endpoints
          websiteLoaded = true
        }
      }
    }

    const targetProductsPerCategory = 400
    const allResults = []

    const storeData = {
      name: 'Gap',
      domain: 'gap.com',
      currency: 'USD',
      country: 'US',
    }

    // Define categories to scrape
    const categories = [
      {
        name: 'Women Shop All',
        gender: 'Women',
        cid: '1127938',
      },
      {
        name: 'Women Dresses',
        gender: 'Women',
        cid: '1127942',
      },
      {
        name: 'Women Tops',
        gender: 'Women',
        cid: '1127943',
      },
      {
        name: 'Women Jeans',
        gender: 'Women',
        cid: '1127944',
      },
      {
        name: 'Men Jeans',
        gender: 'Men',
        cid: '1127945',
      },
      {
        name: 'Men T-Shirts',
        gender: 'Men',
        cid: '1127946',
      },
    ]

    // Check if products file already exists
    let allProducts = []
    let allProductDetails = []
    let productsFilePath = null
    let existingProductsData = await loadProductsFromFile(storeData)

    if (existingProductsData) {
      console.log(`\n🔄 RESUMING FROM EXISTING PRODUCTS FILE`)
      console.log(
        `📄 Found existing products file with ${existingProductsData.data.products.length} products`
      )

      allProducts = existingProductsData.data.products
      productsFilePath = existingProductsData.filePath

      // Count processed vs unprocessed products
      const processedCount = allProducts.filter(
        (p) => p.variants && p.variants.length > 0
      ).length
      const unprocessedCount = allProducts.length - processedCount

      console.log(`✅ Already processed: ${processedCount} products`)
      console.log(`⏳ Remaining to process: ${unprocessedCount} products`)

      // Extract category details from existing products
      const categoryMap = new Map()
      allProducts.forEach((product) => {
        const category = product._category || 'Unknown'
        const gender = product._gender || 'Unknown'
        const key = `${category}-${gender}`

        if (!categoryMap.has(key)) {
          categoryMap.set(key, { category, gender, count: 0 })
        }
        categoryMap.get(key).count++
      })

      allProductDetails = Array.from(categoryMap.values())
    } else {
      console.log(`\n🆕 STARTING FRESH - FETCHING PRODUCTS FROM API`)

      // Scrape each category
      for (const category of categories) {
        console.log(`\n${'='.repeat(50)}`)
        console.log(`🎯 Starting ${category.name} category scraping`)
        console.log(`${'='.repeat(50)}`)

        const categoryProducts = await scrapeGapCategory(
          page,
          category,
          targetProductsPerCategory
        )

        if (categoryProducts.length === 0) {
          console.log(`⚠️ No products found for ${category.name} category`)
          continue
        }

        console.log(
          `\n📦 Found ${categoryProducts.length} ${category.name} products`
        )

        // Add category info to each product for processing
        const categoryProductsWithGender = categoryProducts.map((product) => ({
          ...product,
          _category: category.name,
          _gender: category.gender,
        }))

        allProducts = allProducts.concat(categoryProductsWithGender)

        allProductDetails.push({
          category: category.name,
          gender: category.gender,
          count: categoryProducts.length,
        })
      }

      // Save all products to file before processing details
      if (allProducts.length > 0) {
        console.log(`\n💾 SAVING ${allProducts.length} PRODUCTS TO FILE`)
        productsFilePath = await saveProductsToFile(allProducts, storeData)
      }
    }

    console.log(`\n${'🎯'.repeat(20)}`)
    console.log('🎯 PROCESSING ALL PRODUCTS TOGETHER 🎯')
    console.log(`${'🎯'.repeat(20)}`)
    console.log(`📦 Total products collected: ${allProducts.length}`)
    allProductDetails.forEach((detail) => {
      console.log(`   ${detail.category}: ${detail.count} products`)
    })

    if (allProducts.length === 0) {
      console.log('⚠️ No products found from any category')
      return false
    }

    // Process all products and save to processed.json
    const processingResult = await processProductsToFile(
      allProducts,
      storeData,
      page,
      productsFilePath
    )

    console.log(`\n📊 Processing Results:`)
    console.log(
      `  Products inserted: ${processingResult.mongoResults.inserted}`
    )
    console.log(`  Products skipped: ${processingResult.mongoResults.skipped}`)
    console.log(`  Products errors: ${processingResult.mongoResults.errors}`)

    // Generate catalog files from processed.json
    console.log(`\n${'📚'.repeat(20)}`)
    console.log('📚 GENERATING CATALOG FILES FROM PROCESSED DATA 📚')
    console.log(`${'📚'.repeat(20)}`)

    const catalogResult = await generateCatalogFiles(storeData)

    if (!catalogResult) {
      console.log('⚠️ Failed to generate catalog files')
      return false
    }

    allResults.push({
      categories: allProductDetails,
      totalProducts: allProducts.length,
      jsonPath: catalogResult.jsonPath,
      mongoResults: processingResult.mongoResults,
      storeResult: {},
      totalProductIds: processingResult.productIds.length,
      validationResult: catalogResult.validationResult,
      finalProductCount: catalogResult.totalProducts,
    })

    console.log(`\n${'🎉'.repeat(20)}`)
    console.log('🎉 ALL GAP SCRAPING COMPLETED SUCCESSFULLY! 🎉')
    console.log(`${'🎉'.repeat(20)}`)

    // Summary for combined results
    const combinedResult = allResults[0] // Only one result since we combined everything
    console.log(`\n📊 Combined Results Summary:`)
    console.log(`   Total Products Scraped: ${combinedResult.totalProducts}`)
    console.log(`   Categories Processed:`)
    combinedResult.categories.forEach((cat) => {
      console.log(`     ${cat.category}: ${cat.count} products`)
    })
    console.log(`\n   Processing Stats:`)
    console.log(
      `   MongoDB - Inserted: ${combinedResult.mongoResults.inserted}, Skipped: ${combinedResult.mongoResults.skipped}, Errors: ${combinedResult.mongoResults.errors}`
    )
    console.log(`\n   Validation Stats:`)
    console.log(
      `   Total Processed: ${combinedResult.validationResult.totalCount}`
    )
    console.log(
      `   Valid Products: ${combinedResult.validationResult.validCount}`
    )
    console.log(
      `   Invalid Products: ${combinedResult.validationResult.invalidCount}`
    )
    console.log(
      `   Variants Filtered: ${
        combinedResult.validationResult.totalVariantsFiltered || 0
      }`
    )
    console.log(
      `\n   Final Catalog Products: ${combinedResult.finalProductCount}`
    )
    console.log(`   Output Files: ${combinedResult.jsonPath}`)

    return allResults
  } catch (error) {
    console.error('❌ Error during scraping:', error)
    throw error
  } finally {
    if (browser) {
      // await browser.close()
    }
    // Disconnect from MongoDB
    // await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeGapData()

    if (results && results.length > 0) {
      console.log('\n🎉 Gap products crawling completed successfully!')

      let totalProducts = 0
      let totalInserted = 0
      let totalSkipped = 0
      let totalErrors = 0

      results.forEach((res) => {
        totalProducts += res.totalProducts
        totalInserted += res.mongoResults.inserted
        totalSkipped += res.mongoResults.skipped
        totalErrors += res.mongoResults.errors
      })

      console.log(`📊 Final Summary:`)
      console.log(`   Total products: ${totalProducts}`)
      console.log(
        `   MongoDB - Inserted: ${totalInserted}, Skipped: ${totalSkipped}, Errors: ${totalErrors}`
      )

      // Show category breakdown
      if (results[0] && results[0].categories) {
        console.log(`   Categories:`)
        results[0].categories.forEach((cat) => {
          console.log(`     ${cat.category}: ${cat.count} products`)
        })
      }

      return results
    } else {
      console.log('\n❌ Gap crawling failed')
      return false
    }
  } catch (error) {
    console.error('Error in main function:', error)
    throw error
  }
}

// Run the scraper
if (require.main === module) {
  main()
    .then((result) => {
      if (result) {
        console.log('Script completed successfully')
        process.exit(0)
      } else {
        console.log('Script failed')
        process.exit(1)
      }
    })
    .catch((error) => {
      console.error('Script failed:', error)
      process.exit(1)
    })
}

module.exports = { main, scrapeGapData }
