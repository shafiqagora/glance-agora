// H&M Products Scraper - Men's and Women's Categories
// Uses fetch API to scrape products from HM.com
require('dotenv').config()
const fs = require('fs')
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

// Helper function to get product availability information
const getProductAvailability = async (productId) => {
  try {
    const availabilityUrl = `https://www2.hm.com/hmwebservices/service/product/us/availability/${productId}.json`
    console.log(`Fetching availability info for product: ${productId}`)
    const response = await fetch(availabilityUrl, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        priority: 'u=1, i',
        referer: `https://www2.hm.com/en_us/productpage.${productId}.html`,
        'sec-ch-ua':
          '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
      },
    })

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }

    const availabilityData = await response.json()
    return availabilityData
  } catch (error) {
    console.error(
      `Error fetching product availability for ${productId}:`,
      error.message
    )
    return null
  }
}

// Helper function to process a single product
const processProduct = async (product, gender, category) => {
  const productId = product.id
  console.log(`Processing product: ${product.productName}`)

  // Clean description
  let description = ''

  // Extract materials from product details
  let materials = ''

  const formattedProduct = {
    parent_product_id: productId,
    name: product.productName,
    description: description,
    category: category,
    retailer_domain: 'hm.com',
    brand: 'H&M',
    gender: gender,
    materials: materials,
    return_policy_link:
      'https://www2.hm.com/en_us/customer-service/returns.html',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'hm',
  }

  // Process variants (colors and sizes)
  if (product.swatches && product.swatches.length > 0) {
    for (const swatch of product.swatches) {
      // Get color information
      const colorName = swatch.colorName || swatch.color || ''

      // Get product availability information
      // Get pricing information
      const originalPrice = parseFloat(product.prices[0].maxPrice)
      const salePrice = parseFloat(product.prices[0].minPrice)
      const finalPrice =
        salePrice > 0 && salePrice < originalPrice ? salePrice : originalPrice
      const isOnSale = salePrice > 0 && salePrice < originalPrice

      // Calculate discount percentage
      let discount = calculateDiscount(originalPrice, salePrice)

      // Get images
      let imageUrl = swatch.productImage
      let alternateImages = [swatch.productImage]

      // Get sizes from availability data or product data
      let sizes = []
      if (product.sizes) {
        sizes = product.sizes

        sizes.map((size) => {
          return {
            name: size.label,
            id: size.id,
          }
        })
      }

      if (sizes.length > 0) {
        for (const size of sizes) {
          const sizeName = size.name
          const isInStock = true

          // Build product URL
          let variantUrl = `https://www2.hm.com/en_us/productpage.${swatch.articleId}${size.id}.html`

          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url: variantUrl,
            deeplink_url: variantUrl,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: sizeName,
            color: colorName,
            mpn: uuidv5(
              `${productId}-${colorName}-${sizeName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: 0,
            average_ratings: 0,
            review_count: 0,
            selling_price: originalPrice,
            sale_price: salePrice > 0 ? salePrice : 0,
            final_price: finalPrice,
            discount: discount,
            operation_type: 'INSERT',
            variant_id: uuidv5(
              `${productId}-${swatch.articleId}-${sizeName}-${colorName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
            ),
            variant_description: '',
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
      storeType: 'hm',
      name: 'H&M',
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
        name: storeData.name || 'H&M',
        storeTemplate: 'hm-template',
        storeType: 'hm',
        storeUrl: 'https://www2.hm.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy: 'https://www2.hm.com/en_us/customer-service/returns.html',
        tags: ['men', 'women', 'fashion', 'clothing', 'accessories'],
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
async function scrapeHMCategory(categoryConfig, targetProductCount = 2500) {
  let page = 1
  let allProducts = []
  let hasMoreProducts = true
  const pageSize = 36 // H&M uses 36 products per page

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (hasMoreProducts && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${page}...`)

    try {
      // Build the API URL
      const apiUrl = `https://api.hm.com/search-services/v1/en_us/listing/resultpage?pageSource=PLP&page=${page}&sort=RELEVANCE&pageId=${categoryConfig.pageId}&page-size=${pageSize}&categoryId=${categoryConfig.categoryId}&touchPoint=DESKTOP&skipStockCheck=false`

      console.log(`API URL: ${apiUrl}`)

      // Fetch data from API
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          accept: 'application/json',
          'accept-language': 'en-US,en;q=0.9',
          origin: 'https://www2.hm.com',
          priority: 'u=1, i',
          referer: 'https://www2.hm.com/',
          'sec-ch-ua':
            '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-site',
          'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        },
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const pageData = await response.json()

      // Extract products from the response

      if (pageData.pagination) {
        const { currentPage, totalPages } = pageData.pagination
        hasMoreProducts = currentPage < totalPages
      }
      let pageProducts = []
      if (pageData?.plpList?.productList?.length > 0) {
        pageProducts = pageData.plpList.productList
        console.log(
          `Found ${pageProducts.length} products on ${categoryConfig.name} page ${page}`
        )
        allProducts = allProducts.concat(pageProducts)
        console.log(
          `${categoryConfig.name} total products so far: ${allProducts.length}`
        )

        // Check if we have more products
      } else {
        console.log(`No products found on ${categoryConfig.name} page ${page}`)
        hasMoreProducts = false
      }
    } catch (error) {
      console.error(
        `Error fetching ${categoryConfig.name} page ${page}:`,
        error.message
      )
      hasMoreProducts = false
    }

    // Add a small delay to be respectful to the API
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  // Limit to target product count
  if (allProducts.length > targetProductCount) {
    allProducts = allProducts.slice(0, targetProductCount)
  }

  console.log(`\n✅ ${categoryConfig.name} scraping completed!`)
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

  // Process products sequentially
  console.log(
    `\n📦 Processing ${products.length} products from all categories...`
  )

  for (let i = 0; i < products.length; i++) {
    const product = products[i]
    const gender = product._gender || ''
    const category = product._category || ''

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.productName
      }`
    )

    try {
      const result = await processProduct(product, gender, category)

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

      // Add a small delay between products
      await new Promise((resolve) => setTimeout(resolve, 500))
    } catch (error) {
      console.error(
        `Error processing product ${product.title || product.name}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'hm'
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
      name: storeData.name || 'H&M',
      domain: 'hm.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ['Men', 'Women'],
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

async function scrapeHMProducts() {
  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Starting H&M scraping using fetch API...')

    // Define categories to scrape - H&M category structure
    const categories = [
      {
        name: 'Ladies Clothing',
        gender: 'Women',
        pageId: '/ladies/shop-by-product/view-all',
        categoryId: 'ladies_all',
      },
      {
        name: 'Mens Clothing',
        gender: 'Men',
        pageId: '/men/new-arrivals/view-all',
        categoryId: 'men_newarrivals_all',
      },
      {
        name: 'Kids Clothing',
        gender: 'Kids',
        pageId: '/baby/shop-by-product/clothing/view-all',
        categoryId: 'kids_newbornbaby_viewall',
      },
    ]

    const targetProductsPerCategory = 500
    const allResults = []

    const storeData = {
      name: 'H&M',
      domain: 'hm.com',
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

      const categoryProducts = await scrapeHMCategory(
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
    console.log('🎉 ALL H&M SCRAPING COMPLETED SUCCESSFULLY! 🎉')
    console.log(`${'🎉'.repeat(20)}`)

    // Summary for combined results
    const combinedResult = allResults[0]
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
    const results = await scrapeHMProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 H&M products crawling completed successfully!')

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
      console.log('\n❌ H&M crawling failed')
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

module.exports = { main, scrapeHMProducts }
