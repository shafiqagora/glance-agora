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
} = require('./utils/helper')

axios.defaults.timeout = 180000

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
  const getProductDetails = async (handle) => {
    try {
      // The URL pattern for product details API
      const detailUrl = `https://www.goodamerican.com/en-pk/products/${handle}?_data=routes%2F%28%24locale%29%2Fproducts%2F%24handle`

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
        },
        5,
        1000,
        'US'
      )

      return detailResponse.data
    } catch (error) {
      console.error(
        `Error fetching product details for ${handle}:`,
        error.message
      )
      return null
    }
  }

  // Save or update store entry with product IDs
  async function saveStoreEntry(storeData, productIds) {
    try {
      // Check if store already exists

      // Create new store entry
      const newStore = new Store({
        products: productIds,
        name: storeData.name || 'Good American',
        storeTemplate: 'good-american-template',
        storeType: 'good_american',
        storeUrl: 'https://www.goodamerican.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy:
          storeData.returnPolicy ||
          'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU',
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

  // Helper function to process a single product
  const processProduct = async (product) => {
    console.log(`Processing product: ${product.name}`)

    const productUrl = `https://www.goodamerican.com/en-pk/products/${product.handle}`
    const productId = product.id

    // Get detailed product information
    const currentProduct = await getProductDetails(product.handle)

    const formattedProduct = {
      parent_product_id: productId?.toString(),
      name: product.name,
      description: currentProduct?.product?.description,
      category:
        product.ss_tags
          ?.find((tag) => tag.startsWith('category:'))
          ?.split(':')[1] || '',
      retailer_domain: 'goodamerican.com',
      brand: 'Good American',
      gender: '', // Good American is primarily women's clothing
      materials: '',
      return_policy_link:
        'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU',
      return_policy: '',
      size_chart: '',
      available_bank_offers: '',
      available_coupons: '',
      variants: [],
      operation_type: 'INSERT',
      source: 'good_american',
    }

    // Process variants based on product data
    if (
      product?.variants &&
      product?.variants.nodes &&
      product?.variants.nodes.length > 0
    ) {
      for (const variant of product.variants.nodes) {
        const isInStock = !variant.currentlyNotInStock
        const originalPrice = parseFloat(
          variant.compareAtPriceV2?.amount || variant.priceV2?.amount || 0
        )
        const sellingPrice = parseFloat(variant.priceV2?.amount || 0)
        const salePrice =
          variant.compareAtPriceV2?.amount &&
          variant.compareAtPriceV2?.amount > sellingPrice
            ? sellingPrice
            : null
        const finalPrice = sellingPrice
        const discount =
          variant.compareAtPriceV2?.amount &&
          variant.compareAtPriceV2?.amount > sellingPrice
            ? calculateDiscount(originalPrice, finalPrice)
            : 0
        const isOnSale =
          variant.compare_at_price && variant.compare_at_price > sellingPrice
            ? true
            : false

        // Extract size and color from variant title
        const variantTitle = variant.title || ''
        const size = variant.selectedOptions.find(
          (option) => option.name === 'Size'
        )?.value
        const color = variant.selectedOptions.find(
          (option) => option.name === 'Color'
        )?.value

        // Get images
        let imageUrl = ''
        let alternateImages = []

        if (variant.image?.url) {
          imageUrl = variant.image.url
        }

        // Get alternate images from product images
        if (variant.images && variant.images.length > 0) {
          alternateImages = [product.imageUrl]
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
          size: size,
          color: color,
          mpn: uuidv5(
            `${productId}-${color}-${size}`,
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
          variant_id: variant.id?.toString().split('/').pop(),
          variant_description: '',
        }
        formattedProduct.variants.push(formattedVariant)
      }
    }

    // Save to MongoDB
    try {
      const result = await saveProductToMongoDB(formattedProduct)
      if (result.operation === 'INSERT') {
        mongoResults.inserted++
        // Track product ID for store entry
        if (result.product) {
          productIds.push(result.product._id)
        }
      } else if (result.operation === 'SKIPPED') {
        mongoResults.skipped++
      }
    } catch (error) {
      console.error(
        `Error saving product ${formattedProduct.name}:`,
        error.message
      )
      mongoResults.errors++
    }

    formattedProducts.push(formattedProduct)
  }

  // Process products in chunks to avoid overwhelming the system
  const chunks = chunkArray(products, 10)
  console.log(
    `Processing ${products.length} products in ${chunks.length} chunks...`
  )

  for (let i = 0; i < chunks.length; i++) {
    console.log(`Processing chunk ${i + 1}/${chunks.length}...`)
    const chunk = chunks[i]

    // Process products in parallel within each chunk
    await Promise.all(chunk.map(processProduct))

    // Add delay between chunks to be respectful to the server
    if (i < chunks.length - 1) {
      await new Promise((resolve) => setTimeout(resolve, 2000))
    }
  }

  // Create output directory
  const dirPath = path.join(__dirname, 'output', 'US', 'good_american-US')
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  // Create timestamp for filename
  const filename = `catalog`

  // Create JSON file
  const jsonFilePath = path.join(dirPath, `${filename}.json`)
  fs.writeFileSync(
    jsonFilePath,
    JSON.stringify(formattedProducts, null, 2),
    'utf8'
  )
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, `${filename}.jsonl`)
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
    let currentPage = 1
    const resultsPerPage = 72 // Good American's default per page

    const storeData = {
      name: 'Good American',
      domain: 'goodamerican.com',
      currency: 'USD',
      country: 'US',
      returnPolicy: returnPolicy,
    }

    console.log(`Starting to crawl Good American...`)

    // Parse the base URL to get the API endpoint structure
    const baseApiUrl = API_URL.split('?')[0] // Get base URL without query params

    // Fetch all products by iterating through pages
    let hasMorePages = true
    while (hasMorePages) {
      try {
        console.log(`Fetching page ${currentPage}...`)

        const url = `${baseApiUrl}?page=${currentPage}&bgfilter.collection_handle=clothing`
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
          },
          5,
          1000,
          'US'
        )

        const data = apiResponse.data
        const products = data?.results

        if (!products || products.length === 0) {
          console.log(`No more products found at page ${currentPage}`)
          hasMorePages = false
          break
        } else {
          allProducts.push(...products)
          console.log(
            `Fetched ${products.length} products from page ${currentPage}`
          )

          // Check if we've reached the total number of results
          const totalResults = data?.pagination?.totalResults
          const totalPages = data?.pagination?.totalPages

          if (totalResults && allProducts.length >= totalResults) {
            console.log(`Reached total number of results: ${totalResults}`)
            hasMorePages = false
            break
          }

          if (totalPages && currentPage >= totalPages) {
            console.log(`Reached total pages: ${totalPages}`)
            hasMorePages = false
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
          hasMorePages = false
          break
        }

        // For other errors, try a few more times before giving up
        if (currentPage > 100) {
          // Safety limit
          console.log('Reached safety limit of 100 pages')
          hasMorePages = false
          break
        }
      }

      currentPage += 1

      // Add a small delay between requests to be respectful
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }

    if (allProducts.length === 0) {
      console.log(`No products found`)
      return false
    }

    console.log(`Total products found: ${allProducts.length}`)

    // Generate files
    const result = await generateCSV(allProducts, storeData, API_URL)

    console.log(`Successfully generated files for Good American`)
    console.log(`Files saved at: ${result.jsonPath}`)

    return result
  } catch (e) {
    console.error('Error in scraping Good American store:', e)
    return false
  } finally {
    // Disconnect from MongoDB
    await disconnectDB()
  }
}

// Export the main function
module.exports = {
  main,
}

// If run directly from command line
if (require.main === module) {
  const API_URL = 'https://www.goodamerican.com/en-US/api/searchspring'
  const returnPolicy =
    'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU'

  console.log('Starting Good American crawler...')
  main(API_URL, returnPolicy)
    .then((result) => {
      if (result) {
        console.log('\n🎉 Good American crawling completed successfully!')
        console.log(`Files generated: ${result.jsonPath}`)
        return result.jsonPath
      } else {
        console.log('\n❌ Good American crawling failed')
        process.exit(1)
      }
    })
    .catch((error) => {
      console.error('Error running Good American crawler:', error)
      process.exit(1)
    })
}
