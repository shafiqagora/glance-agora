// Bloomingdales Products Scraper
// Visits Bloomingdales website and API endpoint to parse and save JSON data
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

// Configuration
const BLOOMING_WEBSITE_URL = 'https://www.bloomingdales.com/'

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

// Helper function to process a single product
const processProduct = async (
  product,
  page,
  gender = 'Women',
  category = ''
) => {
  console.log(`Processing product: ${product.detail.name}`)

  const productId = product.id
  const productUrl = `https://bloomingdales.com${product.identifier.productUrl}`

  // Fetch product details from API using Puppeteer
  console.log(`Fetching product details for ID: ${productId}`)
  let productDetails = {}

  try {
    const apiUrl = `https://www.bloomingdales.com/xapi/digital/v1/product/${productId}?_regionCode=US&currencyCode=USD&_customerExperiment=2626-21`

    // Navigate to the API URL
    await page.goto(apiUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 30000,
    })

    // Get the JSON response from the page
    const apiResponse = await page.evaluate(() => {
      return document.body.innerText
    })

    // Parse the JSON response
    const apiData = JSON.parse(apiResponse)

    // Extract product details from the API response
    if (apiData && apiData.product && apiData.product.length > 0) {
      productDetails = apiData.product[0]
      console.log(
        `✅ Successfully fetched details for product: ${product.detail.name}`
      )
    } else {
      console.log(
        `⚠️ No product details found in API response for: ${product.detail.name}`
      )
    }
  } catch (error) {
    console.error(
      `❌ Error fetching product details for ${product.detail.name}:`,
      error.message
    )
    // Continue with empty productDetails if API call fails
  }

  const formattedProduct = {
    parent_product_id: productId,
    name: product.detail.name,
    description: productDetails?.detail?.description,
    category: product.detail.typeName,
    retailer_domain: 'bloomingdales.com',
    brand: product.detail.brand,
    gender: gender,
    materials: productDetails?.detail?.materialsAndCare
      ? productDetails?.detail?.materialsAndCare[0]
      : '',
    return_policy_link:
      'https://customerservice-bloomingdales.com/articles/what-is-the-return-and-exchange-policy',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'blooming',
  }

  // Process variants if product details were successfully fetched
  if (
    productDetails &&
    productDetails.relationships &&
    productDetails.relationships.upcs &&
    Object.keys(productDetails.relationships.upcs).length > 0
  ) {
    const prodTraits = productDetails.traits

    for (const upc of Object.keys(productDetails.relationships.upcs)) {
      let variant = productDetails.relationships.upcs[upc]

      // Get color and size names first
      let colorName = variant.traits.colors.selectedColor
      let sizeName = variant.traits.sizes.selectedSize

      // Map to actual names if available
      if (
        prodTraits &&
        prodTraits.colors &&
        prodTraits.colors.colorMap &&
        prodTraits.colors.colorMap[colorName]
      ) {
        colorName = prodTraits.colors.colorMap[colorName].name
      }
      if (
        prodTraits &&
        prodTraits.sizes &&
        prodTraits.sizes.sizeMap &&
        prodTraits.sizes.sizeMap[sizeName]
      ) {
        sizeName = prodTraits.sizes.sizeMap[sizeName].name
      }

      // Get pricing information
      let originalPrice = 0
      let sellingPrice = 0
      let isOnSale = false

      if (
        prodTraits &&
        prodTraits.colors &&
        prodTraits.colors.colorMap &&
        prodTraits.colors.colorMap[variant.traits.colors.selectedColor]
      ) {
        const colorData =
          prodTraits.colors.colorMap[variant.traits.colors.selectedColor]
        if (colorData.pricing) {
          const regPrice = colorData.pricing.price.tieredPrice.find((item) =>
            item.values.find((ite) => ite.type.toLowerCase().includes('reg'))
          )
          const salePrice = colorData.pricing.price.tieredPrice.find((item) =>
            item.values.find((ite) => ite.type.toLowerCase().includes('sale'))
          )

          originalPrice = regPrice ? regPrice.values[0].value : 0
          sellingPrice =
            salePrice && salePrice.values
              ? salePrice.values[0].value
              : originalPrice

          if (colorData.pricing.price && colorData.pricing.price.priceType) {
            isOnSale = colorData.pricing.price.priceType.onSale
          }
        }
      }

      const finalPrice = sellingPrice > 0 ? sellingPrice : originalPrice
      const discount = calculateDiscount(originalPrice, finalPrice)
      const isInStock = variant.availability
        ? variant.availability.available
        : false

      // Get images
      let imageUrl = ''
      let alternateImages = []

      if (
        prodTraits &&
        prodTraits.colors &&
        prodTraits.colors.colorMap &&
        prodTraits.colors.colorMap[variant.traits.colors.selectedColor]
      ) {
        const colorData =
          prodTraits.colors.colorMap[variant.traits.colors.selectedColor]
        if (colorData.imagery && colorData.imagery.images) {
          let allImages = colorData.imagery.images.map(
            (item) =>
              `https://images.bloomingdalesassets.com/is/image/BLM/products/${item.filePath}`
          )
          imageUrl = allImages[0] || ''
          alternateImages = allImages.slice(1)
        }
      }

      const formattedVariant = {
        price_currency: 'USD',
        original_price: originalPrice,
        link_url: productUrl,
        deeplink_url: productUrl,
        image_url: imageUrl,
        alternate_image_urls: alternateImages,
        is_on_sale: isOnSale,
        is_in_stock: isInStock,
        size: sizeName,
        color: colorName,
        mpn: uuidv5(
          `${product.id}-${colorName}-${sizeName}`,
          '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
        ),
        ratings_count: product?.detail?.reviewStatistics?.aggregate?.count || 0,
        average_ratings:
          product?.detail?.reviewStatistics?.aggregate?.rating || 0,
        review_count: product?.detail?.reviewStatistics?.aggregate?.count || 0,
        selling_price: sellingPrice,
        sale_price: isOnSale ? sellingPrice : 0,
        final_price: finalPrice,
        discount: discount,
        operation_type: 'INSERT',
        variant_id: variant.id,
        variant_description: '',
      }
      formattedProduct.variants.push(formattedVariant)
    }
  } else {
    // If no detailed product info, create a basic variant from the original product data
    console.log(`⚠️ Creating basic variant for product: ${product.detail.name}`)

    const basicVariant = {
      price_currency: 'USD',
      original_price: 0,
      link_url: productUrl,
      deeplink_url: productUrl,
      image_url: product.detail.imageUrl || '',
      alternate_image_urls: [],
      is_on_sale: false,
      is_in_stock: true,
      size: 'One Size',
      color: 'Default',
      mpn: uuidv5(
        `${product.id}-default`,
        '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
      ),
      ratings_count: product.detail.reviewStatistics.aggregate.count || 0,
      average_ratings: product.detail.reviewStatistics.aggregate.rating || 0,
      review_count: product.detail.reviewStatistics.aggregate.count || 0,
      selling_price: 0,
      sale_price: null,
      final_price: 0,
      discount: 0,
      operation_type: 'INSERT',
      variant_id: `${product.id}-default`,
      variant_description: '',
    }
    formattedProduct.variants.push(basicVariant)
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
    // Create new store entry
    const newStore = new Store({
      products: productIds,
      name: storeData.name || 'BloomingDales',
      storeTemplate: 'blooming-template',
      storeType: 'BloomingDales',
      storeUrl: 'https://bloomingdales.com',
      city: '',
      state: '',
      country: storeData.country || 'US',
      isScrapped: true,
      returnPolicy:
        'https://customerservice-bloomingdales.com/articles/what-is-the-return-and-exchange-policy',
      tags: ['women', 'fashion', 'clothing'],
    })

    await newStore.save()
    console.log(`✅ Created new store with ${productIds.length} products`)
    return { operation: 'CREATED', store: newStore }
  } catch (error) {
    console.error('❌ Error saving store entry:', error.message)
    return { operation: 'ERROR', error: error.message }
  }
}

// Helper function to scrape products from a specific Blooming category
async function scrapeBloomingCategory(
  page,
  categoryConfig,
  targetProductCount = 1500
) {
  let currentPage = 1
  let allProducts = []
  let isLastPage = false

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (!isLastPage && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${currentPage}...`)

    // Build API URL with pagination and category ID
    const apiUrl = `https://www.bloomingdales.com/xapi/discover/v1/page?pathname=${categoryConfig.url}&_navigationType=BROWSE&_shoppingMode=SITE&sortBy=ORIGINAL&productsPerPage=60&pageIndex=${currentPage}&_application=SITE&_regionCode=US&currencyCode=USD&size=medium&spItemsVersion=1.1&utagId=0199c77a694500150ccce1ac1e6e05075018706d00d57&visitorId=14218842172999499842047841330611741844&customerId=&_deviceType=DESKTOP`

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

        // Extract products from this page
        if (
          pageData.body?.canvas?.rows[0]?.rowSortableGrid?.zones[1]
            ?.sortableGrid?.collection
        ) {
          pageProducts =
            pageData.body.canvas.rows[0].rowSortableGrid.zones[1].sortableGrid
              .collection

          pageProducts = pageProducts.slice(0, 2)

          pageProducts = pageProducts.map((item) => item.product)
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

    break
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

  // Process products sequentially to avoid overwhelming the browser
  console.log(
    `\n📦 Processing ${products.length} products from all categories sequentially...`
  )

  for (let i = 0; i < products.length; i++) {
    const product = products[i]
    const gender = product._gender || 'Women'
    const category = product._category || 'Unknown'

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.styleName
      }`
    )

    try {
      const result = await processProduct(product, page, gender, category)

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
        `Error processing product ${product.styleName}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'bloomingdales'
  const dirPath = path.join(
    __dirname,
    'output',
    countryCode,
    `${cleanBrandName}-${countryCode}`
  )

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'BloomingDales',
      domain: 'bloomingdales.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ['Women', 'Men', 'Kids', 'Baby'],
      crawled_at: new Date().toISOString(),
    },
    products: formattedProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
  const jsonlContent = formattedProducts
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
  // const storeResult = await saveStoreEntry(storeData, productIds)

  // Log MongoDB results
  console.log(`\n📊 MongoDB Results:`)
  console.log(`  Products inserted: ${mongoResults.inserted}`)
  console.log(`  Products skipped: ${mongoResults.skipped}`)
  console.log(`  Products errors: ${mongoResults.errors}`)
  // console.log(`  Store operation: ${storeResult.operation}`)

  return {
    jsonPath: gzippedFilePath,
    mongoResults,
    storeResult: {},
    totalProductIds: productIds.length,
  }
}

async function scrapeBloomingData() {
  let browser

  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Launching browser for bloomingdales scraping...')
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

    // Step 1: Visit the Blooming website first with retry logic
    console.log('🌐 Visiting Blooming website...')
    const maxRetries = 3
    let retryCount = 0
    let websiteLoaded = false

    while (retryCount < maxRetries && !websiteLoaded) {
      try {
        console.log(
          `Attempt ${retryCount + 1}/${maxRetries} to load Blooming website...`
        )
        await page.goto(BLOOMING_WEBSITE_URL, {
          waitUntil: 'domcontentloaded',
          timeout: 60000,
        })
        websiteLoaded = true
        console.log('✅ Successfully loaded Blooming website')
      } catch (error) {
        retryCount++
        console.log(
          `❌ Failed to load Blooming website (attempt ${retryCount}): ${error.message}`
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
      name: 'Bloomingdales',
      domain: 'bloomingdales.com',
      currency: 'USD',
      country: 'US',
    }

    // Define categories to scrape
    const categories = [
      {
        name: "Women's Clothing",
        gender: 'Women',
        url: '/shop/womens-apparel/all-women&id=1003340',
      },
    ]

    // Collect all products from all categories
    let allProducts = []
    let allProductDetails = []

    // Scrape each category
    for (const category of categories) {
      console.log(`\n${'='.repeat(50)}`)
      console.log(`🎯 Starting ${category.name} category scraping`)
      console.log(`${'='.repeat(50)}`)

      const categoryProducts = await scrapeBloomingCategory(
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
    console.log('🎉 ALL bloomingdales SCRAPING COMPLETED SUCCESSFULLY! 🎉')
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

    return allResults
  } catch (error) {
    console.error('❌ Error during scraping:', error)
    throw error
  } finally {
    if (browser) {
      await browser.close()
    }
    // Disconnect from MongoDB
    // await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeBloomingData()

    if (results && results.length > 0) {
      console.log('\n🎉 Bloomingdale products crawling completed successfully!')

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
      console.log('\n❌ Blooming crawling failed')
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

module.exports = { main, scrapeBloomingData }
