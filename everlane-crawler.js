require('dotenv').config()
const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const fs = require('fs')
const zlib = require('zlib')
const path = require('path')
const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const Store = require('./models/Store')
const {
  useDigitalOceanAI,
  cleanAndTruncate,
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  retryRequestWithProxyRotation,
  createAxiosInstance,
  determineProductDetailsNYDJ,
} = require('./utils/helper')

axios.defaults.timeout = 180000

// Save or update store entry with product IDs
async function saveStoreEntry(storeData, productIds) {
  try {
    // Check if store already exists
    let existingStore = await Store.findOne({
      storeType: 'everlane',
      name: 'Everlane',
      country: storeData.country || 'US',
    })

    // Create new store entry
    const newStore = new Store({
      products: productIds,
      name: storeData.name || 'Everlane',
      storeTemplate: 'everlane-template',
      storeType: 'everlane',
      storeUrl: 'https://www.everlane.com',
      city: '',
      state: '',
      country: storeData.country || 'US',
      isScrapped: true,
      returnPolicy:
        storeData.returnPolicy ||
        'https://support.everlane.com/what-is-your-return-policy-H1fMnra0s',
      tags: ['men', 'women', 'fashion', 'clothing'],
    })

    await newStore.save()
    console.log(`✅ Created new store with ${productIds.length} products`)
    return { operation: 'CREATED', store: newStore }
  } catch (error) {
    console.error('❌ Error saving store entry:', error.message)
    return { operation: 'ERROR', error: error.message }
  }
}

async function generateCSV(products, storeData, baseUrl) {
  const countryCode = storeData.country || 'US'
  const formattedProducts = []
  const productIds = [] // Track product IDs for store entry
  const mongoResults = {
    inserted: 0,
    skipped: 0,
    errors: 0,
  }

  // Helper function to chunk array into smaller arrays
  const chunkArray = (array, chunkSize) => {
    const chunks = []
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize))
    }
    return chunks
  }

  // Helper function to get detailed product information
  const getProductDetails = async (permalink) => {
    try {
      // The URL pattern for product details API
      const detailUrl = `https://www.everlane.com/products/${permalink}.js`

      console.log(`Fetching detailed info from: ${detailUrl}`)

      const detailResponse = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          return await axiosInstance.get(detailUrl, {
            headers: {
              Accept: 'application/json',
              'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              'Accept-Language': 'en-US,en;q=0.9',
              'Accept-Encoding': 'gzip, deflate, br',
            },
          })
        }
      )

      const detailData =
        detailResponse.data?.pageProps?.fallbackData?.products.find(
          (item) => item.permalink == permalink
        )

      return detailData
    } catch (error) {
      console.error(
        `Error fetching product details for ${permalink}:`,
        error.message
      )
      return []
    }
  }

  // Helper function to process a single product
  const processProduct = async (product) => {
    console.log(`Processing product: ${product.data.product_data.display_name}`)

    const productData = product.data.product_data
    const productUrl = product.data.url
    const permalink = productData.permalink

    // Get detailed product information
    const currentProduct = await getProductDetails(permalink)

    const formattedProduct = {
      parent_product_id: productData.id?.toString(),
      name: productData.display_name,
      description: currentProduct.details?.description
        .replace(/<[^>]*>/g, '') // Remove HTML tags
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&nbsp;/g, ' ')
        .replace(/[\u0000-\u001F\u007F-\u009F]/g, '') // Remove control characters
        .replace(/[^\x00-\x7F]/g, '') // Remove non-ASCII characters
        .replace(/\s+/g, ' ') // Normalize whitespace
        .trim(),
      category: '',
      retailer_domain: 'everlane.com',
      brand: 'Everlane',
      gender: currentProduct?.primary_collection?.gender || '',
      materials: currentProduct?.details?.fabric?.care || '',
      return_policy_link:
        'https://support.everlane.com/what-is-your-return-policy-H1fMnra0s',
      return_policy: '',
      size_chart: JSON.stringify(currentProduct.sizeChart),
      available_bank_offers: '',
      available_coupons: '',
      variants: [],
      operation_type: 'INSERT',
      source: 'everlane',
    }

    // Process variants based on detailed product sizes if available
    if (
      productData.product_swatches &&
      productData.product_swatches.length > 0
    ) {
      // Create variant for each size with detailed inventory information
      for (const swatch of productData.product_swatches) {
        const isInStock =
          swatch.orderable_state === 'shippable' ||
          swatch.orderable_state === 'low_stock'

        // Use detailed size information from the product detail API
        const color = swatch.color?.name || 'Default'
        const originalPrice = parseFloat(
          swatch.original_price || swatch.price || 0
        )
        const sellingPrice = parseFloat(swatch.price || 0)
        const salePrice = swatch.final_sale ? sellingPrice : null
        const finalPrice = sellingPrice
        const discount = swatch.final_sale
          ? calculateDiscount(originalPrice, finalPrice)
          : 0
        const isOnSale = swatch.final_sale || false
        const sizes = swatch.sizes || []

        // Get images from detailed product
        let imageUrl = ''
        let alternateImages = []

        if (swatch.albums?.square && swatch.albums.square.length > 0) {
          imageUrl = `https://media.everlane.com/image/upload/c_fill,dpr_2,f_auto,g_face:center,q_auto,w_500/v1/${swatch.albums.square[0].src}`

          // Get alternate images
          alternateImages = swatch.albums.square
            .filter((img) => img.tag !== 'primary')
            .map(
              (img) =>
                `https://media.everlane.com/image/upload/c_fill,dpr_2,f_auto,g_face:center,q_auto,w_500/v1/${img.src}`
            )
            .slice(0, 5)
        }

        for (size of sizes) {
          const formattedVariant = {
            price_currency: 'USD',
            original_price: originalPrice,
            link_url: `https://www.everlane.com/products/${swatch.permalink}`,
            deeplink_url: `https://www.everlane.com/products/${swatch.permalink}`,
            image_url: imageUrl,
            alternate_image_urls: alternateImages,
            is_on_sale: isOnSale,
            is_in_stock: isInStock,
            size: size,
            color: color,
            mpn: uuidv5(
              `${swatch.id}-${color}-${size}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: 0,
            average_ratings: 0,
            review_count: 0,
            selling_price: sellingPrice,
            sale_price: salePrice,
            final_price: finalPrice,
            discount: discount,
            operation_type: 'INSERT',
            variant_id: uuidv5(
              `${productData.id?.toString()}-${swatch.id}-${color}-${size}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
            ),
            variant_description: '',
          }
          formattedProduct.variants.push(formattedVariant)
        }
      }
    }

    const mongoResult = await saveProductToMongoDB(formattedProduct)

    return { formattedProduct, mongoResult }
  }

  // Split products into chunks of 10
  const productChunks = chunkArray(products, 10)

  console.log(
    `Processing ${products.length} products in ${productChunks.length} batches of 10...`
  )

  // Process each chunk of 10 products concurrently
  for (let i = 0; i < productChunks.length; i++) {
    const chunk = productChunks[i]
    console.log(
      `Processing batch ${i + 1}/${productChunks.length} (${
        chunk.length
      } products)...`
    )

    try {
      // Process all products in the current chunk concurrently
      let chunkResults = await Promise.all(
        chunk.map((product) => processProduct(product))
      )

      // Flatten the results since each product can now return multiple products
      chunkResults = chunkResults
        .flat()
        .filter((result) => result.formattedProduct)

      // Collect results from this chunk
      for (const result of chunkResults) {
        formattedProducts.push(result.formattedProduct)

        if (result.mongoResult.operation === 'INSERT') {
          mongoResults.inserted++
          // Track product ID for store entry
          if (result.mongoResult.product) {
            productIds.push(result.mongoResult.product._id)
          }
        } else if (result.mongoResult.operation === 'SKIPPED') {
          mongoResults.skipped++
        } else {
          mongoResults.errors++
        }
      }

      console.log(`Completed batch ${i + 1}/${productChunks.length}`)
    } catch (error) {
      console.error(`Error processing batch ${i + 1}:`, error.message)
      // Continue with next batch even if current batch fails
      mongoResults.errors += chunk.length
    }
  }

  // Create directory structure: countryCode/retailername-countryCode/
  const cleanBrandName = 'everlane'
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
      name: storeData.name || 'Everlane',
      domain: 'everlane.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
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

// Save product to MongoDB
async function saveProductToMongoDB(productData) {
  try {
    // Check if product already exists
    const existingProduct = await Product.findOne({
      parent_product_id: productData.parent_product_id,
    })

    if (existingProduct) {
      console.log(
        `Product ${productData.name} already exists in database, skipping...`
      )
      return { operation: 'SKIPPED', product: existingProduct }
    }

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
    throw error
  }
}

const main = async (API_URL, returnPolicy = '') => {
  try {
    // Connect to MongoDB
    await connectDB()
    let allProducts = []
    let loopCondition = true
    let currentPage = 1
    const resultsPerPage = 20

    const storeData = {
      name: 'Everlane',
      domain: 'everlane.com',
      currency: 'USD',
      country: 'US',
      returnPolicy: returnPolicy,
    }

    console.log(`Starting to crawl Everlane...`)

    // Parse the base URL to get the API endpoint structure
    const baseApiUrl = API_URL.split('?')[0] // Get base URL without query params

    // Fetch all products by iterating through pages
    do {
      try {
        console.log(`Fetching page ${currentPage}...`)

        const url = `${baseApiUrl}?key=key_KQlGTC4GnitM06o7&num_results_per_page=${resultsPerPage}&c=cio-fe-web-everlane&i=20da30e6-9e9e-4d02-adc0-467f59106eae&s=2&page=${currentPage}`

        console.log(url)

        const apiResponse = await retryRequestWithProxyRotation(
          async (axiosInstance) => {
            return await axiosInstance.get(url, {
              headers: {
                Accept: 'application/json',
                'User-Agent':
                  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
              },
            })
          }
        )

        const data = apiResponse.data
        const products = data?.response?.results

        if (!products || products.length === 0) {
          console.log(`No more products found at page ${currentPage}`)
          loopCondition = false
          break
        } else {
          allProducts.push(...products)
          console.log(
            `Fetched ${products.length} products from page ${currentPage}`
          )
          // Check if we've reached the total number of results
          const totalResults = data?.response?.total_num_results
          if (totalResults && allProducts.length >= totalResults) {
            console.log(`Reached total number of results: ${totalResults}`)
            loopCondition = false
            break
          }
        }
      } catch (err) {
        console.log(`Error fetching page ${currentPage}:`, err.message)

        // If we get a 404 or similar error, we've reached the end
        if (
          err.response?.status === 404 ||
          err.message?.includes('404') ||
          err.message?.includes('Not Found')
        ) {
          console.log('Reached end of products (404)')
          loopCondition = false
          break
        }

        // For other errors, try a few more times before giving up
        if (currentPage > 100) {
          // Safety limit
          console.log('Reached safety limit of 100 pages')
          loopCondition = false
          break
        }
      }

      currentPage += 1

      // Add a small delay between requests to be respectful

      await new Promise((resolve) => setTimeout(resolve, 1000))
    } while (loopCondition)

    if (allProducts.length === 0) {
      console.log(`No products found`)
      return false
    }

    console.log(`Total products found: ${allProducts.length}`)

    // Generate files
    const result = await generateCSV(allProducts, storeData, API_URL)

    console.log(`Successfully generated files for Everlane`)
    console.log(`Files saved at: ${result.jsonPath}`)

    return result
  } catch (e) {
    console.error('Error in scraping Everlane store:', e)
    return false
  } finally {
    // Disconnect from MongoDB
    // await disconnectDB()
  }
}

// Export the main function
module.exports = {
  main,
}

// If run directly from command line
if (require.main === module) {
  const API_URL = 'https://ac.cnstrc.com/browse/collection_id/womens-all'
  const returnPolicy = ''

  console.log('Starting Everlane crawler...')
  main(API_URL, returnPolicy)
    .then((result) => {
      if (result) {
        console.log('\n🎉 Everlane crawling completed successfully!')
        console.log(`Files generated: ${result.jsonPath}`)
        return result.jsonPath
      } else {
        console.log('\n❌ Everlane crawling failed')
        process.exit(1)
      }
    })
    .catch((error) => {
      console.error('Error running Everlane crawler:', error)
      process.exit(1)
    })
}
