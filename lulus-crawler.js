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
const { connect } = require('puppeteer-real-browser')

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
    const detailUrl = `https://www.lulus.com/api/product-view/${product.id}`

    console.log(`Fetching detailed info from: ${detailUrl}`)

    const url = `https://www.lulus.com/api/product-view/${product.id}`

    const apiData = await page.evaluate(async (url) => {
      const response = await fetch(url, {
        credentials: 'include',
        headers: {
          accept: 'application/json, text/plain, */*',
        },
      })
      return await response.json()
    }, url)

    // Try to parse the JSON response
    try {
      const detailData = apiData
      return detailData.product
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

// Helper function to process a single product
const processProduct = async (product, page, gender = 'Men', category = '') => {
  const firstSwatch = product.swatches[0]

  console.log(`Processing product: ${firstSwatch.name}`)

  const productId = product.groupId

  // Get detailed product information using the same browser page
  const productDetails = await getProductDetails(firstSwatch, page)

  // Clean description if available
  let description = ''
  if (productDetails?.description) {
    description = sanitizeHtml(productDetails.description || '', {
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

  let materials = productDetails?.descriptionBullets?.materials || ''

  const formattedProduct = {
    parent_product_id: productId,
    name: firstSwatch.name,
    description: description,
    category: category,
    retailer_domain: 'lulus.com',
    brand: 'Lulus',
    gender: gender,
    materials: materials,
    return_policy_link:
      'https://www.lulus.com/customerservice/article/returns-policy',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'lulus',
  }

  // Process variants (colors and sizes)
  if (product.swatches && product.swatches.length > 0) {
    for (const swatch of product.swatches) {
      const colorName = swatch.colorName || 'Default'
      const originalPrice = swatch.productPrice.price // Convert from cents
      const sellingPrice = swatch.productPrice.salePrice || 0
      const finalPrice =
        sellingPrice && sellingPrice > 0 ? sellingPrice : originalPrice
      const discount = calculateDiscount(originalPrice, sellingPrice) // Calculate if there's sale info
      const isOnSale = sellingPrice < originalPrice && sellingPrice > 0

      // Get images for this color
      let imageUrl = ''
      let alternateImages = []

      if (swatch.images && swatch.images.length > 0) {
        // Get the first image as main image
        const mainImage = swatch.images[0]
        if (mainImage.imagePath) {
          imageUrl = `https://www.lulus.com${mainImage.imagePath}`
        }

        // Get alternate images
        alternateImages = swatch.images
          .slice(1, 6) // Take up to 5 alternate images
          .map((img) => `https://www.lulus.com${img.imagePath}`)
      }

      // Get sizes for this color
      const sizes = swatch.sizes || []
      const swatchUrl = `https://www.lulus.com/products/${swatch.name
        .toLowerCase()
        .replace(/\s+/g, '-')}/${swatch.id}.html`

      if (sizes.length > 0) {
        for (const size of sizes) {
          const isInStock = size.quantityType === 'InStock'
          const sizeName =
            productDetails.sizes.find((s) => s.id === size.id)?.name || ''
          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url: swatchUrl,
            deeplink_url: swatchUrl,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: sizeName || '',
            color: colorName,
            mpn: uuidv5(
              `${product.id}-${swatch.id}-${colorName}-${sizeName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: product.reviewCount,
            average_ratings: product.rating,
            review_count: product.reviewCount,
            selling_price: sellingPrice,
            sale_price: sellingPrice,
            final_price: finalPrice,
            discount: discount,
            variant_description: '',
            operation_type: 'INSERT',
            variant_id: uuidv5(
              `${productId}-${swatch.id}-${colorName}-${sizeName}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
            ),
          }
          formattedProduct.variants.push(formattedVariant)
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
    // Create new store entry
    const newStore = new Store({
      products: productIds,
      name: storeData.name || 'Lulus',
      storeTemplate: 'lulus-template',
      storeType: 'lulus',
      storeUrl: 'https://www.lulus.com',
      city: '',
      state: '',
      country: storeData.country || 'US',
      isScrapped: true,
      returnPolicy:
        storeData.returnPolicy ||
        'https://www.lulus.com/customerservice/article/returns-policy',
      tags: ['men', 'fashion', 'clothing'],
    })

    await newStore.save()
    console.log(`✅ Created new store with ${productIds.length} products`)
    return { operation: 'CREATED', store: newStore }
  } catch (error) {
    console.error('❌ Error saving store entry:', error.message)
    return { operation: 'ERROR', error: error.message }
  }
}

// Helper function to scrape products from a specific category
async function scrapeLulusCategory(
  page,
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
    const offset = (currentPage - 1) * 20
    const pageUrl = `${categoryConfig.url}?from=${offset}&size=100`

    const apiData = await page.evaluate(async (url) => {
      const response = await fetch(url, {
        credentials: 'include',
        headers: {
          accept: 'application/json, text/plain, */*',
        },
      })
      return await response.json()
    }, pageUrl)

    console.log(apiData)

    console.log(
      `Extracting content from ${categoryConfig.name} page ${currentPage}...`
    )

    // Try to parse if it's JSON
    let pageData = null
    let pageProducts = []

    try {
      pageData = apiData

      // Check if we have results
      if (!pageData.content?.hasResults || !pageData.content?.products) {
        isLastPage = true
        console.log(
          `${categoryConfig.name} Page ${currentPage}: No more results`
        )
        break
      }

      // Extract products from this page
      if (pageData.content.products && pageData.content.products.length > 0) {
        pageProducts = pageData.content.products

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
    duplicates: 0,
  }

  // Track seen parent_product_ids to avoid duplicates
  const seenParentProductIds = new Set()

  // Filter out products with duplicate parent_product_ids
  const uniqueProducts = products.filter((product) => {
    const productId = product.groupId
    if (seenParentProductIds.has(productId)) {
      console.log(
        `🔄 Skipping duplicate parent_product_id: ${productId} (${product.name})`
      )
      mongoResults.duplicates++
      return false
    }
    seenParentProductIds.add(productId)
    return true
  })

  console.log(
    `\n📦 Processing ${
      uniqueProducts.length
    } unique products from all categories (${
      products.length - uniqueProducts.length
    } duplicates filtered out)...`
  )

  // Process products sequentially to avoid overwhelming the browser
  for (let i = 0; i < uniqueProducts.length; i++) {
    const product = uniqueProducts[i]
    const gender = product._gender || ''
    const category = product._category || ''

    console.log(
      `Processing ${category} product ${i + 1}/${uniqueProducts.length}: ${
        product.name
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
      console.error(`Error processing product ${product.name}:`, error.message)
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'lulus'
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
      name: storeData.name || 'Lulus',
      domain: 'lulus.com',
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
  // const storeResult = await saveStoreEntry(storeData, productIds)

  // Log MongoDB results
  console.log(`\n📊 MongoDB Results:`)
  console.log(`  Products inserted: ${mongoResults.inserted}`)
  console.log(`  Products skipped: ${mongoResults.skipped}`)
  console.log(`  Products errors: ${mongoResults.errors}`)
  console.log(`  Duplicates filtered: ${mongoResults.duplicates}`)
  console.log(`  Store operation: ${storeResult.operation}`)

  return {
    jsonPath: gzippedFilePath,
    mongoResults,
    storeResult: {},
    totalProductIds: productIds.length,
  }
}

async function scrapeLulusProducts() {
  let browser

  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Launching browser for Zara scraping...')
    // browser = await puppeteer.launch({
    //   headless: false,
    //   defaultViewport: null,
    //   args: ['--no-sandbox', '--disable-setuid-sandbox'],
    // })

    const { browser, page } = await connect({
      headless: false,

      args: [],

      turnstile: true,

      connectOption: {},

      disableXvfb: false,
      ignoreAllFlags: false,
    })

    // Set user agent to avoid being blocked
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )

    console.log('Navigating to Lulus.com...')
    await page.goto('https://www.lulus.com/categories/13/dresses.html')

    // Now make the API request from within the page context

    // Define categories to scrape (if you still need this array)
    const categories = [
      {
        name: 'Dresses',
        gender: 'Women',
        url: 'https://www.lulus.com/api/search/products/13',
      },
      {
        name: "Women's Tops",
        gender: 'Women',
        url: 'https://www.lulus.com/api/search/products/10',
      },
      {
        name: 'Bottoms',
        gender: 'Women',
        url: 'https://www.lulus.com/api/search/products/11',
      },
      {
        name: "Women's Clothing",
        gender: 'Women',
        url: 'https://www.lulus.com/api/search/products/7621',
      },
    ]

    const targetProductsPerCategory = 2000
    const allResults = []

    const storeData = {
      name: 'Lulus',
      domain: 'lulus.com',
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

      const categoryProducts = await scrapeLulusCategory(
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
      `   MongoDB - Inserted: ${combinedResult.mongoResults.inserted}, Skipped: ${combinedResult.mongoResults.skipped}, Errors: ${combinedResult.mongoResults.errors}, Duplicates: ${combinedResult.mongoResults.duplicates}`
    )
    console.log(`   Store Operation: ${combinedResult.storeResult.operation}`)
    console.log(`   Total Product IDs: ${combinedResult.totalProductIds}`)

    return allResults
  } catch (error) {
    console.error('❌ Error during scraping:', error)
    throw error
  } finally {
    if (browser) {
      // await browser.close()
    }
    // Disconnect from MongoDB
    await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeLulusProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 Lulus products crawling completed successfully!')

      let totalProducts = 0
      let totalInserted = 0
      let totalSkipped = 0
      let totalErrors = 0
      let totalDuplicates = 0

      results.forEach((res) => {
        totalProducts += res.totalProducts
        totalInserted += res.mongoResults.inserted
        totalSkipped += res.mongoResults.skipped
        totalErrors += res.mongoResults.errors
        totalDuplicates += res.mongoResults.duplicates || 0
      })

      console.log(`📊 Final Summary:`)
      console.log(`   Total products: ${totalProducts}`)
      console.log(
        `   MongoDB - Inserted: ${totalInserted}, Skipped: ${totalSkipped}, Errors: ${totalErrors}, Duplicates: ${totalDuplicates}`
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

module.exports = { main, scrapeLulusProducts }
