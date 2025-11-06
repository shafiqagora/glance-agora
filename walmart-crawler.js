const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const fs = require('fs')
const zlib = require('zlib')
const path = require('path')
const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const {
  useDigitalOceanAI,
  cleanAndTruncate,
  getDomainName,
  calculateDiscount,
  determineProductDetails,
} = require('./utils/helper')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')

// Add your SerpApi key here
const SERPAPI_KEY = process.env.SERPAPI_KEY || 'your_serpapi_key_here'

axios.defaults.timeout = 180000

async function searchWalmartProducts(catId, page = 1) {
  try {
    const response = await axios.get('https://serpapi.com/search', {
      params: {
        engine: 'walmart',
        cat_id: catId,
        api_key: SERPAPI_KEY,
        page: page,
        device: 'desktop',
      },
    })

    return response.data
  } catch (error) {
    console.error(
      `Error searching Walmart products for category "${catId}":`,
      error.message
    )
    throw error
  }
}

async function fetchWalmartProductDetails(productId) {
  try {
    const response = await axios.get('https://serpapi.com/search', {
      params: {
        engine: 'walmart_product',
        product_id: productId,
        api_key: SERPAPI_KEY,
      },
    })

    return response.data.product_result
  } catch (error) {
    console.error(
      `Error fetching Walmart product details for ${productId}:`,
      error.message
    )
    return null
  }
}

async function generateCSV(searchResults, catId, countryCode = 'US') {
  const formattedProducts = []
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  }

  const organicResults = searchResults.organic_results || []

  for (const walmartProduct of organicResults) {
    console.log(`Processing product: ${walmartProduct.title}`)

    try {
      const productDetails = await fetchWalmartProductDetails(
        walmartProduct.product_id
      )

      // Use AI to determine gender, materials, and category
      const aiProductDetails = await determineProductDetails({
        title: walmartProduct.title,
        description: walmartProduct.description || '',
        product_type: '',
        tags: '',
        vendor: 'Walmart',
      })

      if (aiProductDetails.shouldSkip) {
        console.log(
          `Skipping product: ${walmartProduct.title} ${walmartProduct.product_id}`
        )
        continue
      }

      const formattedProduct = {
        parent_product_id: walmartProduct.product_id,
        name: walmartProduct.title,
        description: sanitizeHtml(
          productDetails?.short_description_html || '',
          {
            allowedTags: [],
            allowedAttributes: {},
            textFilter: function (text) {
              return text.replace(/\s+/g, ' ').trim()
            },
          }
        ),
        category: '',
        retailer_domain: 'walmart.com',
        brand: 'Walmart',
        gender: '',
        materials: '',
        return_policy: '',
        return_policy_link:
          'https://www.walmart.com/help/contact-us/return-policy',
        size_chart: '',
        available_bank_offers: '',
        available_coupons: '',
        variants: [],
        operation_type: 'INSERT',
        source: 'walmart',
      }

      // Create variants - Walmart products may have multiple offers or just one main offer
      const variants = []

      if (productDetails.variant_swatches) {
        // Product has multiple variants
        // First, create all variants without MPN
        let productVariants = []

        // Find color and size variant swatches dynamically
        let availableColors = []
        let availableSizes = []

        // Search for color and size variant swatches
        for (let i = 0; i < productDetails.variant_swatches.length; i++) {
          const swatch = productDetails.variant_swatches[i]
          const swatchName = swatch.name?.toLowerCase() || ''

          // Check for color variants
          if (swatchName.includes('color') || swatchName.includes('colour')) {
            colorSwatchIndex = i
            availableColors =
              swatch.available_selections?.map((item) => ({
                name: item.name,
                id: item.id,
              })) || []
          }

          // Check for size variants
          if (
            swatchName.includes('size') ||
            swatchName.includes('clothing size')
          ) {
            sizeSwatchIndex = i
            availableSizes =
              swatch.available_selections?.map((item) => ({
                name: item.name,
                id: item.id,
              })) || []
          }
        }
        if (!availableColors.length || !availableSizes.length) {
          console.log(
            `Skipping product: ${walmartProduct.title} ${walmartProduct.product_id} because no color or size variants found`
          )
          continue
        }

        productVariants =
          productDetails.variant_swatches[0].available_selections
            .map((item) => item.products)
            .flat()
        console.log(productVariants.length, 'variants are')
        for (const variant of productVariants) {
          // Use variant data if available, otherwise use main product data
          const variantData = variant
          // Extract pricing information from productDetails structure
          let originalPrice = variantData.price_map?.price
          let sellingPrice = variantData.price_map?.price
          let salePrice = null

          const finalPrice = salePrice || sellingPrice
          const discount = calculateDiscount(originalPrice, finalPrice)
          const isOnSale = !!salePrice
          const isInStock = variantData.in_stock

          // Find color by checking if any variant ID matches available colors
          const color =
            variantData.variants && availableColors.length > 0
              ? availableColors.find((c) => variantData.variants.includes(c.id))
                  ?.name
              : ''

          // Find size by checking if any variant ID matches available sizes
          const size =
            variantData.variants && availableSizes.length > 0
              ? availableSizes.find((s) => variantData.variants.includes(s.id))
                  ?.name
              : ''

          const prodVariantData = await fetchWalmartProductDetails(
            variantData.product_id
          )

          // Extract images from productDetails structure
          let imageUrl = prodVariantData.images[0]
          let alternateImages = []

          if (productDetails.images && Array.isArray(productDetails.images)) {
            alternateImages = prodVariantData.images.slice(1) // Skip first image as it's the main image
          }

          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url:
              prodVariantData.product_page_url ||
              productDetails.product_page_url ||
              '',
            deeplink_url:
              prodVariantData.product_page_url ||
              productDetails.product_page_url ||
              '',
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: size || '',
            color: color || '',
            mpn: uuidv5(
              `${walmartProduct.product_id}-${color || 'NO_COLOR'}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: walmartProduct.reviews || 0,
            average_ratings: walmartProduct.rating || 0,
            review_count: walmartProduct.reviews || 0,
            selling_price: sellingPrice,
            sale_price: salePrice,
            final_price: finalPrice,
            discount: discount,
            operation_type: 'INSERT',
            variant_id: variantData.product_id,
            variant_description: prodVariantData.short_description_html,
          }

          variants.push(formattedVariant)
        }
      }

      formattedProduct.variants = variants

      // Save product to MongoDB (uncomment if needed)
      try {
        const mongoResult = await saveProductToMongoDB(formattedProduct)
        if (mongoResult.operation === 'INSERT') {
          mongoResults.inserted++
        } else if (mongoResult.operation === 'SKIPPED') {
          mongoResults.skipped++
        }
      } catch (error) {
        console.error(
          `MongoDB save failed for ${formattedProduct.name}:`,
          error.message
        )
        mongoResults.errors++
      }

      formattedProducts.push(formattedProduct)
    } catch (error) {
      console.error(
        `Error processing product ${walmartProduct.title}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure
  const dirPath = path.join(__dirname, 'output', countryCode, 'walmart-US')

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, `catalog.json`)
  const catalogData = {
    products: formattedProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, `catalog.jsonl`)
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

  // Log MongoDB results
  console.log(`\n📊 Processing Results:`)
  console.log(`  Products processed: ${formattedProducts.length}`)
  console.log(`  Products inserted: ${mongoResults.inserted}`)
  console.log(`  Products skipped: ${mongoResults.skipped}`)
  console.log(`  Products errors: ${mongoResults.errors}`)

  return { jsonPath: gzippedFilePath, totalProducts: formattedProducts.length }
}

// Save product to MongoDB (same as Shopify crawler)
async function saveProductToMongoDB(productData) {
  try {
    // Check if product already exists
    const existingProduct = await Product.findOne({
      parent_product_id: productData.parent_product_id,
    })

    if (existingProduct) {
      console.log(
        `Product ${productData.name} already exists in MongoDB. Skipping...`
      )
      return { operation: 'SKIPPED' }
    }

    // Save new product
    const product = new Product(productData)
    await product.save()
    console.log(`✅ Product ${productData.name} saved to MongoDB`)
    return { operation: 'INSERT' }
  } catch (error) {
    console.error(`❌ Error saving product to MongoDB:`, error.message)
    throw error
  }
}

const main = async (catId, maxPages = 500) => {
  if (!SERPAPI_KEY || SERPAPI_KEY === 'your_serpapi_key_here') {
    console.error(
      '❌ Please set your SERPAPI_KEY environment variable or update the script with your API key'
    )
    process.exit(1)
  }

  console.log(`🚀 Starting Walmart crawler for category ID: "${catId}"`)
  console.log(`📄 Max pages to crawl: ${maxPages}`)

  try {
    // Connect to MongoDB
    await connectDB()
    console.log('✅ Connected to MongoDB')

    let allSearchResults = []
    let totalPages = 0

    // First, fetch all products from all pages
    for (let page = 1; page <= maxPages; page++) {
      console.log(`\n📖 Fetching page ${page}...`)

      try {
        let searchResults = await searchWalmartProducts(catId, page)

        if (
          !searchResults.organic_results ||
          searchResults.organic_results.length === 0
        ) {
          console.log(`No more products found on page ${page}. Stopping...`)
          break
        }

        console.log(
          `Found ${searchResults.organic_results.length} products on page ${page}`
        )

        // Add all products from this page to our collection
        allSearchResults.push(...searchResults.organic_results)
        totalPages = page

        // Add delay between pages to respect rate limits
        if (page < maxPages) {
          console.log('⏳ Waiting 2 seconds before next page...')
          await new Promise((resolve) => setTimeout(resolve, 2000))
        }
      } catch (error) {
        console.error(`Error fetching page ${page}:`, error.message)
        break
      }
    }

    console.log(
      `\n📊 Total products collected: ${allSearchResults.length} from ${totalPages} pages`
    )

    // Now process all collected products at once
    if (allSearchResults.length > 0) {
      const combinedSearchResults = {
        organic_results: allSearchResults,
      }

      const result = await generateCSV(combinedSearchResults, catId, 'US')

      console.log(`\n🎉 Crawling completed!`)
      console.log(`📊 Total products processed: ${result.totalProducts}`)
      return { jsonPath: result.jsonPath }
    } else {
      console.log(`\n❌ No products found to process`)
      return { jsonPath: null }
    }
  } catch (error) {
    console.error('❌ Error in main process:', error.message)
    process.exit(1)
  } finally {
    // Disconnect from MongoDB
    await disconnectDB()
    console.log('✅ Disconnected from MongoDB')
  }
}

// Allow script to be run directly or imported
if (require.main === module) {
  const catId = process.argv[2] || '5438' // Default to Home category
  const maxPages = 1

  main(catId, maxPages)
}

module.exports = { main, searchWalmartProducts, generateCSV }
