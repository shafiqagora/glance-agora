// Zara Products Scraper - Men's and Women's Categories
// Scrapes 2,500 products from each category
require('dotenv').config()
const puppeteer = require('puppeteer')
const fs = require('fs')
const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')
const path = require('path')

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

// Helper function to chunk array into smaller arrays
const chunkArray = (array, chunkSize) => {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

// Helper function to get detailed product information using the same browser page
const getProductDetails = async (product, page) => {
  try {
    if (!product.seo || !product.seo.keyword || !product.seo.seoProductId) {
      console.log(`Missing SEO data for product: ${product.name || 'Unknown'}`)
      return null
    }

    // Build the detail URL using the pattern: seo.keyword-seo.seoProductId.html?ajax=true
    const detailUrl = `https://www.zara.com/us/en/${product.seo.keyword}-p${product.seo.seoProductId}.html?ajax=true`

    console.log(`Fetching detailed info from: ${detailUrl}`)

    // Navigate to the detail page using the same browser page
    await page.goto(detailUrl, {
      waitUntil: 'networkidle2',
      timeout: 30000,
    })

    // Get the JSON response from the page
    const bodyText = await page.evaluate(() => {
      return document.body.innerText
    })

    // Try to parse the JSON response
    try {
      const detailData = JSON.parse(bodyText)
      return detailData
    } catch (parseError) {
      console.log(
        `Error parsing JSON response for ${product.name}:`,
        parseError.message
      )
      return null
    }
  } catch (error) {
    console.error(
      `Error fetching product details for ${product.name}:`,
      error.message
    )
    return null
  }
}

// Helper function to process a group of products with the same deep link URL
const processGroupedProduct = async (groupedProducts, page, gender = 'Men') => {
  // Use the first product as the representative for the group
  const representativeProduct = groupedProducts[0]
  console.log(
    `Processing grouped product: ${representativeProduct.name} (${groupedProducts.length} products)`
  )

  const productId = representativeProduct.id?.toString()
  const productUrl = `https://www.zara.com/us/en/${representativeProduct.seo?.keyword}-p${representativeProduct.seo?.seoProductId}.html`

  // Get detailed product information using the representative product
  const productDetails = await getProductDetails(representativeProduct, page)

  // Clean description if available
  let description = ''
  if (productDetails?.product?.seo?.description) {
    description = productDetails.product.seo.description
  }

  let materials =
    productDetails?.product?.detail?.detailedComposition?.parts || ''
  if (materials) {
    let tempMet = ''
    materials = materials.map((item) => {
      tempMet += `${item.description} ${
        item.components.length > 0 ? item.components[0].material : ''
      },`
    })

    materials = tempMet
  }

  const formattedProduct = {
    parent_product_id: productId,
    name: representativeProduct.name,
    description: description,
    category: '',
    retailer_domain: 'zara.com',
    brand: 'Zara',
    gender: gender,
    materials: materials,
    return_policy_link: 'https://www.zara.com/us/en/help-center/HowToReturn',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'zara',
  }

  // Process variants from all products in the group
  if (
    productDetails.product.detail.colors &&
    productDetails.product.detail.colors.length > 0
  ) {
    for (const color of productDetails.product.detail.colors) {
      const colorName = color.name || 'Default'

      // Use the price from the representative product
      const originalPrice = parseFloat(representativeProduct.price) / 100 // Convert from cents
      const sellingPrice = originalPrice // Zara doesn't typically show different sale prices in listing
      const finalPrice = sellingPrice
      const discount = 0 // Calculate if there's sale info
      const isOnSale = false

      // Get images for this color
      let imageUrl = ''
      let alternateImages = []

      if (color.xmedia && color.xmedia.length > 0) {
        // Get the first image as main image
        const mainImage = color.xmedia[0]
        if (mainImage.url) {
          imageUrl = mainImage.url
        }

        // Get alternate images
        alternateImages = color.xmedia
          .slice(1, 6) // Take up to 5 alternate images
          .map((img) => img.url)
      }

      // Get sizes for this color
      const sizes = color.sizes || []

      if (sizes.length > 0) {
        for (const size of sizes) {
          const isInStock = size.availability === 'in_stock'

          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url: productUrl,
            deeplink_url: productUrl,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: size.name || '',
            color: colorName,
            mpn: uuidv5(
              `${representativeProduct.id}-${colorName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: 0,
            average_ratings: 0,
            review_count: 0,
            selling_price: sellingPrice,
            sale_price: null,
            final_price: finalPrice,
            discount: discount,
            operation_type: 'INSERT',
            variant_id: `${productId}-${color.id}-${colorName}-${size.name}`,
            variant_description: '',
          }
          formattedProduct.variants.push(formattedVariant)
        }
      }
    }
  }

  // Remove duplicate variants based on variant_id
  const originalVariantCount = formattedProduct.variants.length
  const seenVariantIds = new Set()
  formattedProduct.variants = formattedProduct.variants.filter((variant) => {
    if (!variant.variant_id || seenVariantIds.has(variant.variant_id)) {
      return false
    }
    seenVariantIds.add(variant.variant_id)
    return true
  })

  const duplicateVariantsRemoved =
    originalVariantCount - formattedProduct.variants.length
  if (duplicateVariantsRemoved > 0) {
    console.log(
      `🔄 Removed ${duplicateVariantsRemoved} duplicate variants for grouped product: ${formattedProduct.name}`
    )
  }

  console.log(
    `📦 Grouped product processed: ${formattedProduct.name} with ${formattedProduct.variants.length} total variants from ${groupedProducts.length} source products`
  )

  const mongoResult = await saveProductToMongoDB(formattedProduct)

  return { formattedProduct, mongoResult }
}

// Helper function to process a single product
const processProduct = async (product, page, gender = 'Men') => {
  console.log(`Processing product: ${product.name}`)

  const productId = product.id?.toString()
  const productUrl = `https://www.zara.com/us/en/${product.seo?.keyword}-p${product.seo?.seoProductId}.html`

  // Get detailed product information using the same browser page
  const productDetails = await getProductDetails(product, page)

  // Clean description if available
  let description = ''
  if (productDetails?.product?.seo?.description) {
    description = productDetails.product.seo.description
  }

  let materials =
    productDetails?.product?.detail?.detailedComposition?.parts || ''
  if (materials) {
    let tempMet = ''
    materials = materials.map((item) => {
      tempMet += `${item.description} ${
        item.components.length > 0 ? item.components[0].material : ''
      },`
    })

    materials = tempMet
  }

  const formattedProduct = {
    parent_product_id: productId,
    name: product.name,
    description: description,
    category: '',
    retailer_domain: 'zara.com',
    brand: 'Zara',
    gender: gender,
    materials: materials,
    return_policy_link: 'https://www.zara.com/us/en/help-center/HowToReturn',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'zara',
  }

  // Process variants (colors and sizes)
  if (
    productDetails.product.detail.colors &&
    productDetails.product.detail.colors.length > 0
  ) {
    for (const color of productDetails.product.detail.colors) {
      const colorName = color.name || 'Default'
      const originalPrice = parseFloat(product.price) / 100 // Convert from cents
      const sellingPrice = originalPrice // Zara doesn't typically show different sale prices in listing
      const finalPrice = sellingPrice
      const discount = 0 // Calculate if there's sale info
      const isOnSale = false

      // Get images for this color
      let imageUrl = ''
      let alternateImages = []

      if (color.xmedia && color.xmedia.length > 0) {
        // Get the first image as main image
        const mainImage = color.xmedia[0]
        if (mainImage.url) {
          imageUrl = mainImage.url
        }

        // Get alternate images
        alternateImages = color.xmedia
          .slice(1, 6) // Take up to 5 alternate images
          .map((img) => img.url)
      }

      // Get sizes for this color
      const sizes = color.sizes || []

      if (sizes.length > 0) {
        for (const size of sizes) {
          const isInStock = size.availability === 'in_stock'

          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url: productUrl,
            deeplink_url: productUrl,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: size.name || '',
            color: colorName,
            mpn: uuidv5(
              `${product.id}-${colorName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: 0,
            average_ratings: 0,
            review_count: 0,
            selling_price: sellingPrice,
            sale_price: null,
            final_price: finalPrice,
            discount: discount,
            operation_type: 'INSERT',
            variant_id: `${productId}-${color.id}-${colorName}-${size.name}`,
            variant_description: '',
          }
          formattedProduct.variants.push(formattedVariant)
        }
      }
    }
  }

  // Remove duplicate variants based on variant_id
  const originalVariantCount = formattedProduct.variants.length
  const seenVariantIds = new Set()
  formattedProduct.variants = formattedProduct.variants.filter((variant) => {
    if (!variant.variant_id || seenVariantIds.has(variant.variant_id)) {
      return false
    }
    seenVariantIds.add(variant.variant_id)
    return true
  })

  const duplicateVariantsRemoved =
    originalVariantCount - formattedProduct.variants.length
  if (duplicateVariantsRemoved > 0) {
    console.log(
      `🔄 Removed ${duplicateVariantsRemoved} duplicate variants for product: ${formattedProduct.name}`
    )
  }

  const mongoResult = await saveProductToMongoDB(formattedProduct)

  return { formattedProduct, mongoResult }
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
      storeType: 'zara',
      name: 'Zara',
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
        name: storeData.name || 'Zara',
        storeTemplate: 'zara-template',
        storeType: 'zara',
        storeUrl: 'https://www.zara.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy:
          storeData.returnPolicy ||
          'https://www.zara.com/us/en/help-center/HowToReturn',
        tags: ['men', 'fashion', 'clothing'],
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

// Helper function to scrape products from a specific category
async function scrapeZaraCategory(
  page,
  categoryConfig,
  targetProductCount = 2500
) {
  let currentPage = 1
  let allProducts = []
  let isLastPage = false

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (!isLastPage && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${currentPage}...`)

    // Navigate to the specific category page with current page number
    await page.goto(`${categoryConfig.url}?ajax=true&page=${currentPage}`)

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
      if (pageData.paginationInfo) {
        isLastPage = pageData.paginationInfo.isLastPage
        console.log(
          `${categoryConfig.name} Page ${currentPage}: isLastPage = ${isLastPage}`
        )
        console.log(
          `Total pages: ${pageData.paginationInfo.totalPages || 'unknown'}`
        )
      }

      // Extract products from this page
      if (pageData.productGroups && pageData.productGroups.length > 0) {
        const productGroup = pageData.productGroups[0]
        if (productGroup.elements) {
          pageProducts = productGroup.elements
            .map((item) => {
              return item.commercialComponents
            })
            .flat()

          console.log(
            `Found ${pageProducts.length} products on ${categoryConfig.name} page ${currentPage}`
          )
          allProducts = allProducts.concat(pageProducts)
          console.log(
            `${categoryConfig.name} total products so far: ${allProducts.length}`
          )
        }
      } else {
        console.log(
          `No product groups found on ${categoryConfig.name} page ${currentPage}`
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

    currentPage++
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

// Generate files for combined products from all categories
async function generateCombinedFiles(products, storeData, page) {
  const countryCode = storeData.country || 'US'
  const formattedProducts = []
  const productIds = [] // Track product IDs for store entry
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  }

  // Group products by their deep link URL
  console.log(`\n📦 Grouping ${products.length} products by deep link URL...`)
  const productGroups = new Map()

  for (const product of products) {
    // Build the deep link URL for this product
    const deepLinkUrl = `https://www.zara.com/us/en/${product.seo?.keyword}-p${product.seo?.seoProductId}.html`

    if (!productGroups.has(deepLinkUrl)) {
      productGroups.set(deepLinkUrl, [])
    }
    productGroups.get(deepLinkUrl).push(product)
  }

  console.log(
    `📦 Grouped into ${productGroups.size} unique products (by deep link URL)`
  )
  console.log(`📦 Original product count: ${products.length}`)
  console.log(`📦 Grouped product count: ${productGroups.size}`)

  // Process grouped products sequentially to avoid overwhelming the browser
  console.log(
    `\n📦 Processing ${productGroups.size} grouped products sequentially...`
  )

  let processedCount = 0
  for (const [deepLinkUrl, groupedProducts] of productGroups) {
    processedCount++

    // Use the first product as the representative for the group
    const representativeProduct = groupedProducts[0]
    const gender = representativeProduct._gender || 'Unisex'
    const category = representativeProduct._category || 'Unknown'

    console.log(
      `Processing grouped product ${processedCount}/${productGroups.size}: ${representativeProduct.name} (${groupedProducts.length} variants)`
    )

    try {
      const result = await processGroupedProduct(groupedProducts, page, gender)

      if (result.formattedProduct) {
        formattedProducts.push(result.formattedProduct)

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
        `Error processing grouped product ${representativeProduct.name}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'zara'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  // Filter out invalid products
  const filterResults = filterValidProducts(formattedProducts)
  const finalProducts = filterResults.validProducts

  console.log(
    `📊 Validation: ${filterResults.validCount} valid, ${filterResults.invalidCount} invalid out of ${filterResults.totalCount} total products`
  )

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'Zara',
      domain: 'zara.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: finalProducts.length,
      original_products_scraped: filterResults.totalCount,
      invalid_products_filtered: filterResults.invalidCount,
      categories: ['Men', 'Women'],
      crawled_at: new Date().toISOString(),
    },
    products: finalProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
  const jsonlContent = finalProducts
    .map((product) => JSON.stringify(product))
    .join('\n')
  fs.writeFileSync(jsonlFilePath, jsonlContent, 'utf8')
  console.log(`JSONL file generated: ${jsonlFilePath}`)

  // Gzip the JSONL file
  const gzippedFilePath = `${jsonlFilePath}.gz`
  const jsonlBuffer = fs.readFileSync(jsonlFilePath)
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
  fs.writeFileSync(gzippedFilePath, gzippedBuffer)
  console.log(`Gzipped JSONL file generated: ${gzippedFilePath}`)

  // Create or update store entry with all product IDs
  console.log('\n📦 Creating/updating store entry...')
  const storeResult = await saveStoreEntry(storeData, productIds)

  // Log MongoDB results
  console.log(`\n📊 MongoDB Results:`)
  console.log(`  Products inserted: ${mongoResults.inserted}`)
  console.log(`  Products skipped: ${mongoResults.skipped}`)
  console.log(`  Products errors: ${mongoResults.errors}`)
  console.log(`  Store operation: ${storeResult.operation}`)

  return {
    jsonPath: gzippedFilePath,
    mongoResults,
    storeResult,
    totalProductIds: productIds.length,
    filterResults,
  }
}

async function scrapeZaraProducts() {
  let browser

  try {
    // Connect to MongoDB
    await connectDB()

    console.log('🚀 Launching browser for Zara scraping...')
    browser = await puppeteer.launch({
      headless: true,
      defaultViewport: null,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
    })

    const page = await browser.newPage()

    // Set user agent to avoid being blocked
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    console.log('Navigating to Zara.com...')
    await page.goto('https://www.zara.com')

    // Define categories to scrape
    const categories = [
      {
        name: 'Men',
        gender: 'Men',
        url: 'https://www.zara.com/us/en/man-all-products-l7465.html',
      },
      {
        name: 'Dresses',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-dresses-l1066.html?v1=2420896',
      },
      {
        name: 'Tops',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-tops-l1322.html?v1=2419940',
      },
      {
        name: 'Skirts',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-skirts-l1299.html?v1=2420454',
      },
      {
        name: 'Cardigans & Sweaters',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-cardigans-sweaters-l8322.html?v1=2419844',
      },
      {
        name: 'Outerwear',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-outerwear-vests-l1204.html?v1=2583005',
      },
      {
        name: 'T-Shirts',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-tshirts-l1362.html?v1=2420417',
      },
      {
        name: 'Shorts',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-shorts-denim-l1710.html?v1=2574489',
      },
      {
        name: 'Jeans',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-jeans-l1119.html?v1=2419185',
      },
      {
        name: 'Trousers',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-trousers-l1335.html?v1=2420795',
      },
      {
        name: 'Knitwear',
        gender: 'Women',
        url: 'https://www.zara.com/us/en/woman-knitwear-l1152.html?v1=2420306',
      },
    ]

    const targetProductsPerCategory = 1000
    const allResults = []

    const storeData = {
      name: 'Zara',
      domain: 'zara.com',
      currency: 'USD',
      country: 'US',
    }

    // Collect all products from both categories
    let allProducts = []
    let allProductDetails = []

    // Scrape each category
    for (const category of categories) {
      console.log(`\n${'='.repeat(50)}`)
      console.log(`🎯 Starting ${category.name} category scraping`)
      console.log(`${'='.repeat(50)}`)

      const categoryProducts = await scrapeZaraCategory(
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

    console.log(`\n${'🎯'.repeat(20)}`)
    console.log('🎯 PROCESSING ALL PRODUCTS TOGETHER 🎯')
    console.log(`${'🎯'.repeat(20)}`)
    console.log(`📦 Total products collected: ${allProducts.length}`)
    allProductDetails.forEach((detail) => {
      console.log(`   ${detail.category}: ${detail.count} products`)
    })

    // Remove duplicate products based on parent_product_id (product.id)
    const originalProductCount = allProducts.length
    const seenProductIds = new Set()
    allProducts = allProducts.filter((product) => {
      const productId = product.id?.toString()
      if (!productId || seenProductIds.has(productId)) {
        return false
      }
      seenProductIds.add(productId)
      return true
    })

    const duplicateProductsRemoved = originalProductCount - allProducts.length
    if (duplicateProductsRemoved > 0) {
      console.log(
        `🔄 Removed ${duplicateProductsRemoved} duplicate products based on parent_product_id`
      )
    }
    console.log(`📦 Products after deduplication: ${allProducts.length}`)

    if (allProducts.length === 0) {
      console.log('⚠️ No products found from any category')
      return false
    }

    // Process all products together and generate combined files
    const combinedFilesResult = await generateCombinedFiles(
      allProducts,
      storeData,
      page
    )

    allResults.push({
      categories: allProductDetails,
      totalProducts: allProducts.length,
      jsonPath: combinedFilesResult.jsonPath,
      mongoResults: combinedFilesResult.mongoResults,
      storeResult: combinedFilesResult.storeResult,
      totalProductIds: combinedFilesResult.totalProductIds,
    })

    console.log(`\n${'🎉'.repeat(20)}`)
    console.log('🎉 ALL ZARA SCRAPING COMPLETED SUCCESSFULLY! 🎉')
    console.log(`${'🎉'.repeat(20)}`)

    // Summary for combined results
    const combinedResult = allResults[0] // Only one result since we combined everything
    console.log(`\n📊 Combined Results Summary:`)
    console.log(`   Total Products: ${combinedResult.totalProducts}`)
    console.log(`   Categories Processed:`)
    combinedResult.categories.forEach((cat) => {
      console.log(`     ${cat.category}: ${cat.count} products`)
    })
    console.log(`   Output Files: ${combinedResult.jsonPath}`)
    console.log(
      `   MongoDB - Inserted: ${combinedResult.mongoResults.inserted}, Skipped: ${combinedResult.mongoResults.skipped}, Errors: ${combinedResult.mongoResults.errors}`
    )
    console.log(`   Store Operation: ${combinedResult.storeResult.operation}`)
    console.log(`   Total Product IDs: ${combinedResult.totalProductIds}`)

    if (combinedResult.filterResults) {
      console.log(
        `✅ Final: ${combinedResult.filterResults.validCount} valid products saved (${combinedResult.filterResults.invalidCount} filtered out)`
      )
    }

    return allResults
  } catch (error) {
    console.error('❌ Error during scraping:', error)
    throw error
  } finally {
    if (browser) {
      await browser.close()
    }
    // Disconnect from MongoDB
    await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeZaraProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 Zara products crawling completed successfully!')

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
      console.log('\n❌ Zara crawling failed')
      return false
    }
  } catch (error) {
    console.error('Error in main function:', error)
    throw error
  }
}

// Run the scraper
if (require.main === module) {
  // Check if we're running in test mode

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

module.exports = { main, scrapeZaraProducts }
