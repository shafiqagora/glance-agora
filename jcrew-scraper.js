// J.Crew Products Scraper - Women's Categories
// Uses Constructor.io search API to scrape products from jcrew.com
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

// Helper function to fetch detailed product information from J.Crew availability API
const fetchProductDetails = async (productId) => {
  try {
    const apiUrl = `https://www.jcrew.com/browse/products/${productId}?expand=availability%2Cvariations%2Cprices%2Cset_products&display=all&country-code=US`

    console.log(`Fetching detailed info for product: ${productId}`)

    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        accept: 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        origin: 'https://www.jcrew.com',
        referer: 'https://www.jcrew.com/',
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
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const productData = await response.json()
    return productData
  } catch (error) {
    console.error(
      `Error fetching product details for ${productId}:`,
      error.message
    )
    return null
  }
}

// Helper function to process a single product with detailed information
const processProduct = async (product, gender, category) => {
  const productId = product.data.id || product.data.familyId
  console.log(`Processing product: ${product.value}`)

  // Fetch detailed product information
  const productDetails = await fetchProductDetails(productId)

  fs.writeFileSync(
    'productDetails.json',
    JSON.stringify(productDetails, null, 2)
  )
  fs.writeFileSync('productData.json', JSON.stringify(product, null, 2))
  return
  if (!productDetails) {
    console.log(
      `⚠️ Could not fetch details for product ${productId}, skipping...`
    )
    return { formattedProduct: null, mongoResult: {} }
  }

  // Clean description - use detailed description if available
  let description =
    productDetails.long_description || product.data.description || ''

  // Extract materials from short_description

  const formattedProduct = {
    parent_product_id: productId,
    name: productDetails.name || product.value,
    description: description,
    category: product.data.defaultCategoryId,
    retailer_domain: 'jcrew.com',
    brand: 'J.Crew',
    gender: product.data.productGender,
    materials: '',
    return_policy_link:
      'https://www.jcrew.com/help/returns-exchanges?srsltid=AfmBOorJK4AzGzzpVoVf0nCEvGkGpXgDDOIqYTTS47k96v4QYoGtsBkI',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'jcrew',
  }

  // Process variants using the detailed product information
  if (productDetails.variants && productDetails.variants.length > 0) {
    // Get color attributes for mapping
    const colorMap = {}
    if (productDetails.variation_attributes) {
      const colorAttribute = productDetails.variation_attributes.find(
        (attr) => attr.id === 'color'
      )
      if (colorAttribute) {
        colorAttribute.values.forEach((colorValue) => {
          colorMap[colorValue.value] = colorValue.name
        })
      }
    }
    const variationsMap = product.variations_map

    if (variationsMap) {
      // Process variations for orderable items (true key in variations_map)
      const orderableVariations = variationsMap.true || {}

      for (const fitType in orderableVariations) {
        const fitVariations = orderableVariations[fitType]

        for (const colorCode in fitVariations) {
          const colorVariant = fitVariations[colorCode]

          const colorName = colorVariant.colorName

          // Get color name

          // Get pricing information
          const originalPrice = parseFloat(colorVariant.max_price)
          const sellingPrice = parseFloat(colorVariant.min_price)
          const finalPrice = sellingPrice > 0 ? sellingPrice : originalPrice
          const isOnSale = colorVariant.max_discount > 0 ? true : false

          // Check stock availability

          // Build image URL using J.Crew's image facade
          const imageUrl = colorVariant.main_image_url

          // Build product URL
          const variantUrl = productDetails.variants.find(
            (variant) => variant.product_id === colorVariant.variation_id
          )?.link

          const allSizes = productDetails.variants.filter(
            (variant) => variant.product_id === colorVariant.variation_id
          )

          for (const size of allSizes) {
            const sizeName = size.variation_values.size
            const isInStock = size.orderable

            const formattedVariant = {
              price_currency: 'USD',
              original_price: originalPrice,
              link_url: variantUrl,
              deeplink_url: variantUrl,
              image_url: imageUrl,
              alternate_image_urls: [imageUrl],
              is_on_sale: isOnSale,
              is_in_stock: isInStock,
              size: sizeName,
              color: colorName,
              mpn: uuidv5(
                `${productId}-${colorCode}-${sizeName}`,
                '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
              ),
              ratings_count: 0, // Not available in this API
              average_ratings: 0, // Not available in this API
              review_count: 0, // Not available in this API
              selling_price: originalPrice,
              sale_price: isOnSale ? finalPrice : 0,
              final_price: finalPrice,
              discount: colorVariant.max_discount, // Calculate if needed
              operation_type: 'INSERT',
              variant_id: uuidv5(
                `${productId}-${colorVariant.variation_id}-${colorCode}-${sizeName}`,
                '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
              ),
              variant_description: '',
            }
            formattedProduct.variants.push(formattedVariant)
          }
        }
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
      storeType: 'jcrew',
      name: 'J.Crew',
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
        name: storeData.name || 'J.Crew',
        storeTemplate: 'jcrew-template',
        storeType: 'jcrew',
        storeUrl: 'https://www.jcrew.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy:
          'https://www.jcrew.com/help/returns-exchanges?srsltid=AfmBOorJK4AzGzzpVoVf0nCEvGkGpXgDDOIqYTTS47k96v4QYoGtsBkI',
        tags: ['women', 'fashion', 'clothing', 'accessories', 'preppy'],
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

// Helper function to scrape products from J.Crew using Constructor.io API
async function scrapeJCrewCategory(categoryConfig, targetProductCount = 2500) {
  let page = 1
  let allProducts = []
  let hasMoreProducts = true
  const numResultsPerPage = 5 // J.Crew uses 60 products per page

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (hasMoreProducts && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${page}...`)

    try {
      // Build the Constructor.io API request
      const apiUrl = `https://ac.cnstrc.com/browse/group_id/${categoryConfig.groupId}`

      const params = new URLSearchParams({
        c: 'ciojs-client-2.51.0',
        key: 'key_GZ67TnLoJ8IV4vZ2',
        i: '1e574bd2-b004-492c-ba6e-6c88352f2816',
        s: '1',
        page: page.toString(),
        num_results_per_page: numResultsPerPage.toString(),
        'filters[orderable]': 'True',
        'filters[allowedCountries]': 'US',
        'filters[group_id]': categoryConfig.groupId,
        'filters[isAppExclusive]': 'False',
        'filters[displayOn]': 'standard_usd',
        variations_map: JSON.stringify({
          group_by: [
            { name: 'orderable', field: 'data.orderable' },
            { name: 'productSizingName', field: 'data.productSizingName' },
            { name: 'color', field: 'data.color' },
          ],
          values: {
            min_price: { aggregation: 'min', field: 'data.price' },
            max_price: { aggregation: 'max', field: 'data.price' },
            main_image_url: { aggregation: 'first', field: 'data.image_url' },
            count: { aggregation: 'field_count', field: 'data.stockLevel' },
            colorName: { aggregation: 'first', field: 'data.masterColor' },
            size: { aggregation: 'first', field: 'data.size' },
            color: { aggregation: 'first', field: 'data.color' },
            masterColor: { aggregation: 'first', field: 'data.masterColor' },
            skuShotType: { aggregation: 'first', field: 'data.skuShotType' },
            min_discount: { aggregation: 'min', field: 'data.discountValue' },
            max_discount: { aggregation: 'max', field: 'data.discountValue' },
            badges: { aggregation: 'first', field: 'data.badges' },
            variation_id: { aggregation: 'first', field: 'data.variation_id' },
            stockLevel: { aggregation: 'all', field: 'data.stockLevel' },
            displayOn: { aggregation: 'first', field: 'data.displayOn' },
          },
          dtype: 'object',
        }),
        _dt: Date.now().toString(),
      })

      const fullUrl = `${apiUrl}?${params.toString()}`

      // Fetch data from Constructor.io API
      const response = await fetch(fullUrl, {
        method: 'GET',
        headers: {
          accept: '*/*',
          'accept-language': 'en-US,en;q=0.9',
          origin: 'https://www.jcrew.com',
          referer: 'https://www.jcrew.com/',
          'sec-ch-ua':
            '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'cross-site',
          'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        },
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const pageData = await response.json()

      // Extract products from the response
      let pageProducts = []
      if (pageData.response && pageData.response.results) {
        pageProducts = pageData.response.results
        console.log(
          `Found ${pageProducts.length} products on ${categoryConfig.name} page ${page}`
        )

        allProducts = allProducts.concat(pageProducts)
        allProducts = allProducts.slice(0, 1)
        console.log(
          `${categoryConfig.name} total products so far: ${allProducts.length}`
        )

        // Check if we have more pages based on result count
        if (pageProducts.length < numResultsPerPage) {
          hasMoreProducts = false
        } else {
          page++
        }
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

    // Remove this break for full scraping
    break
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
    const gender = product._gender || 'Women'
    const category = product._category || ''

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.value || product.data?.value
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
      } else {
        // Product was skipped (no details available)
        mongoResults.skipped++
      }

      // Add a delay between products to be respectful to API
      await new Promise((resolve) => setTimeout(resolve, 1000))
    } catch (error) {
      console.error(
        `Error processing product ${product.value || product.data?.value}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'jcrew'
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
      name: storeData.name || 'J.Crew',
      domain: 'jcrew.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ['Women'],
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

async function scrapeJCrewProducts() {
  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Starting J.Crew scraping using Constructor.io API...')

    // Define categories to scrape - J.Crew category structure
    const categories = [
      {
        name: "Women's Clothing",
        gender: 'Women',
        groupId: 'womens~categories~clothing',
      },
      // {
      //   name: "Men's Clothing",
      //   gender: 'men',
      //   groupId: 'mens~categories~clothing',
      // },
    ]

    const targetProductsPerCategory = 1
    const allResults = []

    const storeData = {
      name: 'J.Crew',
      domain: 'jcrew.com',
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

      const categoryProducts = await scrapeJCrewCategory(
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
    console.log('🎉 ALL J.CREW SCRAPING COMPLETED SUCCESSFULLY! 🎉')
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
    const results = await scrapeJCrewProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 J.Crew products crawling completed successfully!')

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
      console.log('\n❌ J.Crew crawling failed')
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

module.exports = { main, scrapeJCrewProducts }
