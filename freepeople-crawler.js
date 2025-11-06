// FreePeople Products Scraper - Women's Categories
// Scrapes products from multiple categories
require('dotenv').config()
const fs = require('fs')
const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')
const path = require('path')
const { connect } = require('puppeteer-real-browser')
const puppeteer = require('puppeteer-extra')

// add stealth plugin and use defaults (all evasion techniques)
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
  retryRequestWithProxyRotation,
  createAxiosInstance,
} = require('./utils/helper')

// Helper function to chunk array into smaller arrays
const chunkArray = (array, chunkSize) => {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
}

// Helper function to get detailed product information using the same browser page
const getProductDetails = async (product) => {
  try {
    const detailUrl = `https://api.freepeople.com/api/catalog/v1/fp-us/pools/US_DIRECT/products?slug=${product.productSlug}&projection-slug=pdp&req-info=pdp&countryCode=US`
    // Make the API request using retryRequestWithProxyRotation
    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        return await axiosInstance.get(detailUrl, {
          headers: {
            accept: 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            authorization:
              'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJmcCIsImV4cCI6MTc2MDQxNjU1NS41OTA3OTg2LCJpYXQiOjE3NjA0MTU5NTUuNTkwNzk4NiwiZGF0YSI6IntcImNyZWF0ZWRUaW1lXCI6IDE3NTA2NjQ3ODAuMDE5MDI5LCBcInByb2ZpbGVJZFwiOiBcImE4a1dzSkZJNFBrR2NPZzRtcHJIbEwxMEtsWXBuZEo3NjVtbUVZNGxQTnEwZVlQaVdhbkNtNy8yUkgxQmMzcDY1VSszcnMycnhoam00cEQzMENjSUJ3PT0wNmI5MDNiMWU1OTI4NzkxMDRiZmZjNDU0M2U3ZDkzMTdkZTgzNjIyYzhlMTk4ZjljZDAzYzJmMTI1OTJhNDM2XCIsIFwiYW5vbnltb3VzXCI6IHRydWUsIFwidHJhY2VyXCI6IFwiM1E2NVRLUFg1UlwiLCBcInNjb3BlXCI6IFtcIkdVRVNUXCJdLCBcInNpdGVJZFwiOiBcImZwLXVzXCIsIFwiYnJhbmRJZFwiOiBcImZwXCIsIFwic2l0ZUdyb3VwXCI6IFwiZnBcIiwgXCJkYXRhQ2VudGVySWRcIjogXCJVUy1OVlwiLCBcImdlb1JlZ2lvblwiOiBcIkFTLVNHXCIsIFwiZWRnZXNjYXBlXCI6IHtcInJlZ2lvbkNvZGVcIjogXCJQQlwifSwgXCJjYXJ0SWRcIjogXCJwdXVaUlgxUHBLNDhpU2ZsU2dEMFhJSTNCR1FUcEFsRmhlRXhjalVGc2lzcVlGOUkySWtLRXFBNzNzZTRGV0JvNVUrM3JzMnJ4aGptNHBEMzBDY0lCdz09MDU4YjJlYTczM2QxNjk1ZTAzOTI1ZDBmMTljYTRhZmU4NTk5MmZkNGYwNzE1YTdiNDc3NzJjZGE4MTE1ZmQ0M1wifSJ9.9kqIiaDp2cjhbNTTlSM_Yf7oLhH-bH19w21YRoan9MI',
            origin: 'https://www.freepeople.com',
            referer: 'https://www.freepeople.com/',
            'sec-ch-ua':
              '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
            'x-urbn-channel': 'web',
            'x-urbn-country': 'US',
            'x-urbn-currency': 'USD',
            'x-urbn-experience': 'ss',
            'x-urbn-geo-region': 'AS-SG',
            'x-urbn-language': 'en-US',
            'x-urbn-primary-data-center-id': 'US-NV',
            'x-urbn-site-id': 'fp-us',
          },
        })
      },
      2, // maxRetries
      2000, // baseDelay
      'US' // country
    )

    const apiData = response.data

    try {
      const detailData = apiData
      return detailData[0] // Return first item from array
    } catch (parseError) {
      console.log(
        `Error parsing JSON response for ${product.displayName}:`,
        parseError.message
      )
      return null
    }
  } catch (error) {
    console.error(
      `Error fetching product details for ${product.displayName}:`,
      error.message
    )
    return null
  }
}

// Helper function to process a single product
const processProduct = async (product, gender = 'Women', category = '') => {
  console.log(`Processing product: ${product.product.displayName}`)

  let origProduct = product.product
  const productId = origProduct.productId

  // Get detailed product information using the same browser page
  const productDetails = await getProductDetails(origProduct)
  // Clean description if available
  let description = ''
  if (productDetails?.product?.longDescription) {
    description = sanitizeHtml(productDetails.product.longDescription || '', {
      allowedTags: [],
      allowedAttributes: {},
    })
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&nbsp;/g, ' ')
      .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // Remove control characters
      .replace(/[^\x00-\x7F]/g, '') // Remove non-ASCII characters
      .replace(/\s+/g, ' ') // Normalize whitespace
      .trim()
  }

  let materials = productDetails?.product?.contents?.join(', ') || ''

  const formattedProduct = {
    parent_product_id: productId,
    name: product.product.displayName,
    description: description,
    category: category,
    retailer_domain: 'freepeople.com',
    brand: productDetails?.product?.brand || 'Free People',
    gender: gender,
    materials: materials,
    return_policy_link: 'https://www.freepeople.com/help/returns-exchanges/',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'freepeople',
  }

  // Process variants (colors and sizes)
  if (productDetails?.skuInfo?.primarySlice?.sliceItems) {
    for (const colorItem of productDetails.skuInfo.primarySlice.sliceItems) {
      const colorName = colorItem.displayName || 'Default'
      const originalPrice = productDetails.skuInfo.listPriceHigh || 0
      const sellingPrice = productDetails.skuInfo.salePriceHigh || 0
      const finalPrice = sellingPrice > 0 ? sellingPrice : originalPrice
      const discount = calculateDiscount(originalPrice, sellingPrice)
      const isOnSale = sellingPrice < originalPrice && sellingPrice > 0

      // Get images for this color
      let imageUrl = ''
      let alternateImages = []

      if (colorItem.images && colorItem.images.length > 0) {
        // Get the first image as main image
        const mainImage = colorItem.images[0]
        if (mainImage) {
          imageUrl = `https://images.urbndata.com/is/image/FreePeople/${productDetails.product.styleNumber}_${colorItem.code}_${mainImage}`
        }

        // Get alternate images
        alternateImages = colorItem.images
          .slice(1, 6) // Take up to 5 alternate images
          .map(
            (img) =>
              `https://images.urbndata.com/is/image/FreePeople/${productDetails.product.styleNumber}_${colorItem.code}_${img}`
          )
      }

      // Get sizes for this color
      const skus = colorItem.includedSkus || []
      const productUrl = `https://www.freepeople.com/products/${origProduct.productSlug}`

      if (skus.length > 0) {
        for (const sku of skus) {
          const isInStock = sku.stockLevel !== 0
          const sizeName = sku.size || ''
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
              `${productId}-${colorItem.code}-${colorName}-${sizeName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: productDetails.reviews?.count || 0,
            average_ratings: productDetails.reviews?.averageRating || 0,
            review_count: productDetails.reviews?.count || 0,
            selling_price: sellingPrice,
            sale_price: sellingPrice,
            final_price: finalPrice,
            discount: discount,
            variant_description: '',
            operation_type: 'INSERT',
            variant_id: uuidv5(
              `${productId}-${colorItem.code}-${colorName}-${sizeName}-${colorItem.id}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
            ),
          }
          formattedProduct.variants.push(formattedVariant)
        }
      }
    }
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
      storeType: 'freepeople',
      name: 'Free People',
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
        name: storeData.name || 'Free People',
        storeTemplate: 'freepeople-template',
        storeType: 'freepeople',
        storeUrl: 'https://www.freepeople.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy: 'https://www.freepeople.com/help/returns-exchanges/',
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

// Helper function to scrape products from a specific category
async function scrapeFreePeopleCategory(
  categoryConfig,
  targetProductCount = 1200
) {
  let currentPage = 1
  let allProducts = []
  let isLastPage = false

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (!isLastPage && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${currentPage}...`)

    // Calculate the correct offset for pagination
    const offset = (currentPage - 1) * 72
    const pageUrl = categoryConfig.url
    // Make the API request using retryRequestWithProxyRotation
    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        return await axiosInstance.post(
          pageUrl,
          {
            pageSize: 250,
            skip: offset,
            projectionSlug: 'categorytiles',
            personalization: '0',
            customerConsent: 'true',
            featureProductIds: [],
          },
          {
            headers: {
              accept: 'application/json, text/plain, */*',
              'accept-language': 'en-US,en;q=0.9',
              authorization:
                'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJmcCIsImV4cCI6MTc2MDQxNjU1NS41OTA3OTg2LCJpYXQiOjE3NjA0MTU5NTUuNTkwNzk4NiwiZGF0YSI6IntcImNyZWF0ZWRUaW1lXCI6IDE3NTA2NjQ3ODAuMDE5MDI5LCBcInByb2ZpbGVJZFwiOiBcImE4a1dzSkZJNFBrR2NPZzRtcHJIbEwxMEtsWXBuZEo3NjVtbUVZNGxQTnEwZVlQaVdhbkNtNy8yUkgxQmMzcDY1VSszcnMycnhoam00cEQzMENjSUJ3PT0wNmI5MDNiMWU1OTI4NzkxMDRiZmZjNDU0M2U3ZDkzMTdkZTgzNjIyYzhlMTk4ZjljZDAzYzJmMTI1OTJhNDM2XCIsIFwiYW5vbnltb3VzXCI6IHRydWUsIFwidHJhY2VyXCI6IFwiM1E2NVRLUFg1UlwiLCBcInNjb3BlXCI6IFtcIkdVRVNUXCJdLCBcInNpdGVJZFwiOiBcImZwLXVzXCIsIFwiYnJhbmRJZFwiOiBcImZwXCIsIFwic2l0ZUdyb3VwXCI6IFwiZnBcIiwgXCJkYXRhQ2VudGVySWRcIjogXCJVUy1OVlwiLCBcImdlb1JlZ2lvblwiOiBcIkFTLVNHXCIsIFwiZWRnZXNjYXBlXCI6IHtcInJlZ2lvbkNvZGVcIjogXCJQQlwifSwgXCJjYXJ0SWRcIjogXCJwdXVaUlgxUHBLNDhpU2ZsU2dEMFhJSTNCR1FUcEFsRmhlRXhjalVGc2lzcVlGOUkySWtLRXFBNzNzZTRGV0JvNVUrM3JzMnJ4aGptNHBEMzBDY0lCdz09MDU4YjJlYTczM2QxNjk1ZTAzOTI1ZDBmMTljYTRhZmU4NTk5MmZkNGYwNzE1YTdiNDc3NzJjZGE4MTE1ZmQ0M1wifSJ9.9kqIiaDp2cjhbNTTlSM_Yf7oLhH-bH19w21YRoan9MI',
              'content-type': 'application/json',
              origin: 'https://www.freepeople.com',
              referer: 'https://www.freepeople.com/',
              'sec-ch-ua':
                '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"',
              'sec-fetch-dest': 'empty',
              'sec-fetch-mode': 'cors',
              'sec-fetch-site': 'same-site',
              'user-agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
              'x-urbn-channel': 'web',
              'x-urbn-country': 'US',
              'x-urbn-currency': 'USD',
              'x-urbn-experience': 'ss',
              'x-urbn-geo-region': 'AS-SG',
              'x-urbn-language': 'en-US',
              'x-urbn-pool': 'US_DIRECT',
              'x-urbn-primary-data-center-id': 'US-NV',
              'x-urbn-site-id': 'fp-us',
            },
          }
        )
      },
      2, // maxRetries
      2000, // baseDelay
      'US' // country
    )

    const apiData = response.data
    console.log(`API Response Status: ${response.status}`)

    console.log(
      `Extracting content from ${categoryConfig.name} page ${currentPage}...`
    )

    // Try to parse if it's JSON
    let pageData = null
    let pageProducts = []

    try {
      pageData = apiData

      // Check if we have results
      if (!pageData.records || pageData.records.length === 0) {
        isLastPage = true
        console.log(
          `${categoryConfig.name} Page ${currentPage}: No more results`
        )
        break
      }

      // Extract products from this page
      if (pageData.records && pageData.records.length > 0) {
        pageProducts = pageData.records.map((item) => item.allMeta.tile)

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
        isLastPage = true
      }

      console.log(
        `Successfully parsed JSON data for ${categoryConfig.name} page ${currentPage}`
      )
    } catch (e) {
      console.log(
        `Error parsing JSON for ${categoryConfig.name} page ${currentPage}:`,
        e.message
      )
      isLastPage = true
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
async function generateCombinedFiles(products, storeData) {
  const countryCode = storeData.country || 'US'
  const formattedProducts = []
  const productIds = [] // Track product IDs for store entry
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  }

  // Process products in batches of 50 to optimize performance
  console.log(
    `\n📦 Processing ${products.length} products from all categories in batches of 50...`
  )

  const batchSize = 50
  const batches = chunkArray(products, batchSize)

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const batch = batches[batchIndex]
    console.log(
      `Processing batch ${batchIndex + 1}/${batches.length} (${
        batch.length
      } products)`
    )

    // Process all products in the current batch concurrently
    const batchPromises = batch.map(async (product, index) => {
      const gender = product._gender || ''
      const category = product._category || ''
      const globalIndex = batchIndex * batchSize + index + 1

      console.log(
        `Processing ${category} product ${globalIndex}/${products.length}: ${product.product.displayName}`
      )

      try {
        const result = await processProduct(product, gender, category)
        return { success: true, result, product }
      } catch (error) {
        console.error(
          `Error processing product ${product.product.displayName}:`,
          error.message
        )
        return { success: false, error: error.message, product }
      }
    })

    // Wait for all products in the batch to complete
    const batchResults = await Promise.all(batchPromises)

    // Process the results
    batchResults.forEach(({ success, result, error, product }) => {
      if (success && result.formattedProduct) {
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
      } else {
        mongoResults.errors++
      }
    })

    console.log(
      `Batch ${batchIndex + 1}/${batches.length} completed. Progress: ${
        (batchIndex + 1) * batchSize
      }/${products.length} products processed`
    )
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'freepeople'
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
      name: storeData.name || 'Free People',
      domain: 'freepeople.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
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
  }
}

async function scrapeFreePeopleProducts() {
  try {
    // Connect to MongoDB
    await connectDB()

    // Define categories to scrape
    const categories = [
      {
        name: "Women's Dresses",
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/womens-clothes',
      },
      {
        name: 'Jeans',
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/jeans',
      },
      {
        name: "Women's Activewear",
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/all-activewear',
      },
      {
        name: "Women's Swimwear",
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/all-swimwear',
      },
      {
        name: 'Lingerie',
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/intimates',
      },
    ]

    const targetProductsPerCategory = 500
    const allResults = []

    const storeData = {
      name: 'Free People',
      domain: 'freepeople.com',
      currency: 'USD',
      country: 'US',
    }

    // Collect all products from all categories
    let allProducts = []
    let allProductDetails = []

    // Scrape each category
    for (const category of categories) {
      console.log(`\n${'='.repeat(50)}`)
      console.log(`🎯 Starting ${category.name} category scraping`)
      console.log(`${'='.repeat(50)}`)

      const categoryProducts = await scrapeFreePeopleCategory(
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
      storeData
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
    console.log('🎉 ALL FREEPEOPLE SCRAPING COMPLETED SUCCESSFULLY! 🎉')
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
    // Disconnect from MongoDB
    await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeFreePeopleProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 FreePeople products crawling completed successfully!')

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
      console.log('\n❌ FreePeople crawling failed')
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
        // process.exit(0)
      } else {
        console.log('Script failed')
        // process.exit(1)
      }
    })
    .catch((error) => {
      console.error('Script failed:', error)
      // process.exit(1)
    })
}

module.exports = { main, scrapeFreePeopleProducts }
