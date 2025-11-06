// Aritzia Products Scraper - Women's Categories
// Uses Algolia search API to scrape products from aritzia.com
require('dotenv').config()
const fs = require('fs')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')
const path = require('path')
const puppeteer = require('puppeteer')
const admin = require('firebase-admin')

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

// Global browser instance for image downloading
let browser = null

// Firebase configuration - Replace with your actual Firebase project credentials
// To get these values:
// 1. Go to Firebase Console > Project Settings > Service Accounts
// 2. Click "Generate new private key"
// 3. Download the JSON file and copy the values below
const firebaseConfig = {
  type: 'service_account',
  project_id: 'glance-1aa4e',
  private_key_id: 'f21b130c870d324f0e5d16b06733fb0e00b9f29b',
  private_key:
    '-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDA1sqWNcLORbi3\nI8Lzfr7wjoCDb2cABRo3wAk7K2gGNDjjqfWekXaBs7FDrdod0NeDzrwmgGVhjqE7\nDpSENadF/RRC0L9JAdBLeJ6xxhDKyanSUzokQAElfPmE8PgFWkWYa2kn0ktoiMOP\nMY9hnrMfymQvVWuM7amrXcTgduNifhP/jgxbmt07di65lM6x8MKX3G6GNaaAMLu7\nIgosddbag9gQ+i5PgngsoCnqcPokB0K9Lw/QptH2l8bWsNZvxOnShNvPE6JA95vO\nCmGtw9kdNSkkJHtc02urXBixtcICPYbgkohfjCsAOxKRKbB9necRP8j3AyvpSNOj\nYGNm8TSNAgMBAAECggEABu2wtFo/eDnOZoYaaF/oFwHdDDE3JNZfbXFbjjdZCCAI\nNeXStV0fQjm6khFFAkfmoQ25z4hSxtaLucYkVTu4RC4mSYrxKdEEMtylQa3kHI/H\np47l9Td3fb6nwxGwAkE5ZD3iYVPk8bSNki+C/0w8aKqqPtPK3B1jOIz97d1XqMo0\nO7g5hdVVp87BsF2WX9vxswa1eRGmsugJXpHUu3dJIss65cfsS93JCWvF/w5HQY1+\n3lFEszwdaUvm7cpIFNph9QopuQ2tYtf3TvIFK3Ucp3gnzoaDCi5Qo9tvdSk07KI4\nUxww5ehf1/u+34j788Ab8mMakt0YsZiTDHzOY9iO4QKBgQDfRLdTTyvDxdkKFOGn\nTlMqXEYYZ9uow5F1L56DGq8d9lbjdTDV5LhqOZ62IHFfgqGWQFmD+FhWz0iP6Ae0\nzfBjs7R2QivVjIMexSZNzW2Sf/rL7HdB+DTtL20hSb10/xHYsVIkx2LgnKteDAYi\nOis5n16OAto2ygRQ4KeViAkmfwKBgQDdHA6v+3PwT7wAf6ssTjMGOYNv8hbBOkua\ne8zO+UaoI8X+pVZPmPNFHtcv7hBQI6KC54KXPfrp5RCRNBwr35r1g0IuHaQguavr\nUq0gCYwwXz3sAHwOswpT62ZX4oi+my6e0bIumcMxarWE6ARx6pShk50jsO8qnBXo\njWRqES1W8wKBgQCBxSL5HS9G3xxDq9dssy3LlU54qQUXlnRZNJBhi4T1WVUvZ9I2\nwHYf59XD7h93cCoTdtuQxSwzaM+4NRklkf2DNdRSWCu6N62OmPCmkBx75j8TgCHh\nCi9ZHRPFIWMpOYhZ9tbT4cAq5WUFiN+XzU+KcoM275l99vWDPNCrLSKEaQKBgDxx\n+yHo390GLyMDw8us5Q4Ckwf7anUQdrKYvXindMVqYionEN1ZtsdikvkrX5JI65hV\nqcGm0/00/b50PG9SBY6wf1sUB1Za68C9LU7gzU47+zWVPFTYZS5j3+qOp0tUKP6J\n3OgYaOANh+n5c52gv1kcfYzQRdGkMekNHjJQ19A5AoGALSJ8+pMhVQQJlM6CFe4H\nv66czuoxwcLWAcFQ/O6vTFz6+mNig2gretGWKwX2r6DY2tYxANK1lbXK/I9I8i4c\nhxglwVxCttEXCg4t1FT/oYCoRV6Exi12CWsQ1YZpawMU2jEl5uwFpNeM06aR1S8G\n+nYxsHc72LrRvZXluw4W8RU=\n-----END PRIVATE KEY-----\n',
  client_email: 'firebase-adminsdk-fbsvc@glance-1aa4e.iam.gserviceaccount.com',
  client_id: '117866978784870170931',
  auth_uri: 'https://accounts.google.com/o/oauth2/auth',
  token_uri: 'https://oauth2.googleapis.com/token',
  auth_provider_x509_cert_url: 'https://www.googleapis.com/oauth2/v1/certs',
  client_x509_cert_url:
    'https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40glance-1aa4e.iam.gserviceaccount.com',
  universe_domain: 'googleapis.com',
}

// Replace with your Firebase Storage bucket name (usually your-project-id.appspot.com)
const firebaseStorageBucket = 'glance-1aa4e.firebasestorage.app'

let firebaseInitialized = false

// Initialize Firebase Admin SDK
function initializeFirebase() {
  if (!firebaseInitialized) {
    try {
      // Check if Firebase is already initialized
      if (!admin.apps.length) {
        admin.initializeApp({
          credential: admin.credential.cert(firebaseConfig),
          storageBucket: firebaseStorageBucket,
        })
      }
      firebaseInitialized = true
      console.log('✅ Firebase initialized successfully')
    } catch (error) {
      console.error('❌ Error initializing Firebase:', error.message)
      throw error
    }
  }
}

// Upload image to Firebase Storage
async function uploadImageToFirebase(localImagePath, fileName) {
  try {
    initializeFirebase()

    const bucket = admin.storage().bucket()
    const destination = `aritzia-images/${fileName}`

    // Upload the file
    await bucket.upload(localImagePath, {
      destination: destination,
      metadata: {
        metadata: {
          firebaseStorageDownloadTokens: uuidv4(), // Generate download token
        },
      },
    })

    // Get the download URL
    const file = bucket.file(destination)
    const [url] = await file.getSignedUrl({
      action: 'read',
      expires: '03-01-2500', // Long expiry date
    })

    console.log(`✅ Uploaded to Firebase: ${fileName}`)
    return url
  } catch (error) {
    console.error(`❌ Error uploading ${fileName} to Firebase:`, error.message)
    return null
  }
}

// Enhanced function to save image locally and upload to Firebase
async function saveImageLocallyAndUploadToFirebase(
  imageUrl,
  productId,
  colorId,
  imageIndex = 0
) {
  try {
    // First, save image locally (existing functionality)
    const localPath = await saveImageLocally(
      imageUrl,
      productId,
      colorId,
      imageIndex
    )

    if (!localPath) {
      console.log(`Failed to save image locally: ${imageUrl}`)
      return null
    }

    // Create Firebase filename
    const urlParts = imageUrl.split('/')
    const originalFileName = urlParts[urlParts.length - 1]
    const extension = path.extname(originalFileName) || '.webp'
    const firebaseFileName = `${productId}_${colorId}_${imageIndex}${extension}`

    // Get absolute path for upload
    const absoluteLocalPath = path.resolve(__dirname, localPath)

    // Upload to Firebase
    const firebaseUrl = await uploadImageToFirebase(
      absoluteLocalPath,
      firebaseFileName
    )

    if (firebaseUrl) {
      // Delete local file after successful upload to Firebase
      try {
        fs.unlinkSync(absoluteLocalPath)
        console.log(`🗑️ Deleted local file: ${firebaseFileName}`)
      } catch (deleteError) {
        console.warn(
          `⚠️ Could not delete local file ${absoluteLocalPath}:`,
          deleteError.message
        )
      }
      return firebaseUrl
    }

    // If Firebase upload fails, delete local file and return null (will fallback to original URL)
    try {
      fs.unlinkSync(absoluteLocalPath)
      console.log(
        `🗑️ Deleted local file after Firebase upload failed: ${firebaseFileName}`
      )
    } catch (deleteError) {
      console.warn(
        `⚠️ Could not delete local file ${absoluteLocalPath}:`,
        deleteError.message
      )
    }
    return null
  } catch (error) {
    console.error(
      `Error in saveImageLocallyAndUploadToFirebase:`,
      error.message
    )
    // Clean up any local file that might have been created
    try {
      const localPath = await saveImageLocally(
        imageUrl,
        productId,
        colorId,
        imageIndex
      )
      if (localPath) {
        const absoluteLocalPath = path.resolve(__dirname, localPath)
        if (fs.existsSync(absoluteLocalPath)) {
          fs.unlinkSync(absoluteLocalPath)
          console.log(
            `🗑️ Cleaned up local file after error: ${productId}_${colorId}_${imageIndex}`
          )
        }
      }
    } catch (cleanupError) {
      console.warn(`⚠️ Error during cleanup:`, cleanupError.message)
    }
    return null
  }
}

// Initialize browser for image downloading
async function initBrowser() {
  if (!browser) {
    browser = await puppeteer.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
      ],
    })
  }
  return browser
}

// Close browser when done
async function closeBrowser() {
  if (browser) {
    await browser.close()
    browser = null
  }
}

// Clean up empty local images directory
async function cleanupLocalImagesDirectory() {
  try {
    const imagesDir = path.join(__dirname, 'images', 'aritzia-US')

    // Check if directory exists
    if (fs.existsSync(imagesDir)) {
      // Check if directory is empty
      const files = fs.readdirSync(imagesDir)
      if (files.length === 0) {
        // Remove empty directory
        fs.rmdirSync(imagesDir)
        console.log('🗑️ Removed empty local images directory')

        // Also remove parent images directory if it's empty
        const parentImagesDir = path.join(__dirname, 'images')
        if (fs.existsSync(parentImagesDir)) {
          const parentFiles = fs.readdirSync(parentImagesDir)
          if (parentFiles.length === 0) {
            fs.rmdirSync(parentImagesDir)
            console.log('🗑️ Removed empty parent images directory')
          }
        }
      } else {
        console.log(
          `⚠️ Local images directory not empty, contains ${files.length} files`
        )
      }
    }
  } catch (error) {
    console.warn('⚠️ Error cleaning up local images directory:', error.message)
  }
}

// Function to download image using Puppeteer
async function downloadImageWithPuppeteer(imageUrl, savePath) {
  try {
    const browserInstance = await initBrowser()
    const page = await browserInstance.newPage()

    // Set user agent to avoid blocking
    await page.setUserAgent(
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
    )

    // Navigate to the image URL
    const response = await page.goto(imageUrl, {
      waitUntil: 'networkidle0',
      timeout: 30000,
    })

    if (!response.ok()) {
      throw new Error(`HTTP ${response.status()}: ${response.statusText()}`)
    }

    // Get the image buffer
    const imageBuffer = await response.buffer()

    // Ensure directory exists
    const dir = path.dirname(savePath)
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true })
    }

    // Save the image
    fs.writeFileSync(savePath, imageBuffer)

    await page.close()
    return true
  } catch (error) {
    console.error(`Error downloading image ${imageUrl}:`, error.message)
    return false
  }
}

// Function to create local image path and download image
async function saveImageLocally(imageUrl, productId, colorId, imageIndex = 0) {
  try {
    // Create images directory structure
    const imagesDir = path.join(__dirname, 'images', 'aritzia-US')
    if (!fs.existsSync(imagesDir)) {
      fs.mkdirSync(imagesDir, { recursive: true })
    }

    // Extract file extension from URL or default to .webp
    const urlParts = imageUrl.split('/')
    const fileName = urlParts[urlParts.length - 1]
    const extension = path.extname(fileName) || '.webp'

    // Create unique filename
    const localFileName = `${productId}_${colorId}_${imageIndex}${extension}`
    const localPath = path.join(imagesDir, localFileName)

    // Download the image
    const success = await downloadImageWithPuppeteer(imageUrl, localPath)

    if (success) {
      // Return relative path for use in the catalog
      return path.join('images', 'aritzia-US', localFileName)
    }

    return null
  } catch (error) {
    console.error(`Error saving image locally:`, error.message)
    return null
  }
}

// Helper function to process a single product
const processProduct = async (product, gender, category) => {
  const productId = product.masterId
  console.log(`Processing product: ${product.name}`)

  // Clean description
  let description = ''

  const formattedProduct = {
    parent_product_id: productId,
    name: product.name,
    description: description,
    category:
      product.categories && product.categories.length > 0
        ? product.categories[0]
        : category,
    retailer_domain: 'aritzia.com',
    brand: product.brand || 'Aritzia',
    gender: gender,
    materials:
      product.fabric && product.fabric.length > 0 ? product.fabric[0] : '',
    return_policy_link: 'https://www.aritzia.com/intl/en/returns',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'aritzia',
  }

  const variantsToProcess = product.selectableColors

  if (variantsToProcess && variantsToProcess.length > 0) {
    for (const variant of variantsToProcess) {
      // Get color information
      let colorName = variant.value

      // Get pricing information
      const originalPrice = parseFloat(product.price.max || 0)
      const salePrice = parseFloat(variant.prices[0].prices[0] || 0)
      const finalPrice =
        salePrice > 0 && salePrice < originalPrice ? salePrice : originalPrice
      const isOnSale = salePrice > 0 && salePrice < originalPrice

      // Calculate discount percentage
      let discount = calculateDiscount(originalPrice, salePrice)

      const colorid = Object.keys(variant.colorIds)[0]
      const variantImages = variant.colorIds[colorid]

      let imageUrl = ''
      let localImageUrl = ''
      if (variantImages && variantImages.length > 0) {
        const originalImageUrl = `https://assets.aritzia.com/image/upload/c_crop,ar_1920:2623,g_south/q_auto,f_auto,dpr_auto,w_900/${variantImages[0]}`

        // Download and save image locally, then upload to Firebase
        console.log(
          `Downloading and uploading main image for ${product.name} - ${colorName}`
        )
        localImageUrl = await saveImageLocallyAndUploadToFirebase(
          originalImageUrl,
          productId,
          colorid,
          0
        )

        // Use Firebase URL if upload successful, otherwise fallback to original URL
        imageUrl = localImageUrl || originalImageUrl
      }

      let alternateImages = [imageUrl]
      if (variantImages && Array.isArray(variantImages)) {
        console.log(
          `Downloading ${variantImages.length} alternate images for ${product.name} - ${colorName}`
        )

        const localAlternateImages = []
        for (let i = 0; i < variantImages.length; i++) {
          const originalUrl = `https://assets.aritzia.com/image/upload/c_crop,ar_1920:2623,g_south/q_auto,f_auto,dpr_auto,w_900/${variantImages[i]}`

          // Download each alternate image and upload to Firebase
          const localUrl = await saveImageLocallyAndUploadToFirebase(
            originalUrl,
            productId,
            colorid,
            i
          )

          // Use Firebase URL if upload successful, otherwise use original URL
          localAlternateImages.push(localUrl || originalUrl)

          // Add small delay between downloads to be respectful
          await new Promise((resolve) => setTimeout(resolve, 500))
        }

        alternateImages = localAlternateImages
      }

      const sizes = variant.sizeRun

      for (size of sizes) {
        const isInStock = true

        // Build product URL
        let variantUrl = `https://www.aritzia.com/intl/en/product/${product.slug}?color=${colorid}`

        const formattedVariant = {
          price_currency: 'USD',
          original_price: originalPrice,
          link_url: variantUrl,
          deeplink_url: variantUrl,
          image_url: imageUrl,
          alternate_image_urls: alternateImages,
          is_on_sale: isOnSale,
          is_in_stock: isInStock,
          size: size,
          color: colorName,
          mpn: uuidv5(
            `${productId}-${colorName}`,
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
            `${productId}-${colorid}-${size}`,
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
      storeType: 'aritzia',
      name: 'Aritzia',
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
        name: storeData.name || 'Aritzia',
        storeTemplate: 'aritzia-template',
        storeType: 'aritzia',
        storeUrl: 'https://www.aritzia.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy: 'https://www.aritzia.com/intl/en/returns',
        tags: ['women', 'fashion', 'clothing', 'accessories', 'luxury'],
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

// Helper function to scrape products from Aritzia using Algolia search
async function scrapeAritziaCategory(
  categoryConfig,
  targetProductCount = 2500
) {
  let page = 0
  let allProducts = []
  let hasMoreProducts = true
  const hitsPerPage = 100 // Aritzia uses 120 products per page

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (hasMoreProducts && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${page + 1}...`)

    try {
      // Build the Algolia API request
      const apiUrl =
        'https://search-0.aritzia.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%3B%20autocomplete-core%20(1.18.1)%3B%20autocomplete-js%20(1.18.1)'

      const requestBody = {
        requests: [
          {
            indexName:
              'production_ecommerce_aritzia__Aritzia_INTL__products__default',
            query: categoryConfig.query || '',
            params: `hitsPerPage=${hitsPerPage}&page=${page}&maxValuesPerFacet=100&facets=%5B%22activity%22%2C%22articleFit%22%2C%22brand%22%2C%22buyingCode%22%2C%22fabric%22%2C%22feature%22%2C%22inseam%22%2C%22legShape%22%2C%22length%22%2C%22neckline%22%2C%22occasion%22%2C%22price.discount%22%2C%22productSupport%22%2C%22refinementColor%22%2C%22rise%22%2C%22sizeRun%22%2C%22shippableSizes%22%2C%22sleeve%22%2C%22stitching%22%2C%22stretch%22%2C%22style%22%2C%22subDept%22%2C%22sustainability%22%2C%22trend%22%2C%22vgDiscountGroup%22%2C%22warmth%22%2C%22wash%22%2C%22readyToShip%22%2C%22storeAvailability.id%22%5D&getRankingInfo=true&clickAnalytics=true&filters=${
              categoryConfig.filters
            }&facetFilters=%5B%5D&ruleContexts=%5B%22${
              categoryConfig.ruleContext || 'clothing'
            }%22%5D`,
          },
        ],
      }

      console.log(`Algolia API URL: ${apiUrl}`)

      // Fetch data from Algolia API
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          accept: '*/*',
          'accept-language': 'en-US,en;q=0.9',
          'content-type': 'application/x-www-form-urlencoded',
          origin: 'https://www.aritzia.com',
          referer: 'https://www.aritzia.com/',
          'sec-ch-ua':
            '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-site',
          'user-agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
          'x-algolia-api-key': '1455bca7c6c33e746a0f38beb28422e6',
          'x-algolia-application-id': 'SONLJM8OH6',
          'x-algolia-usertoken': '839968272-1752782008',
        },
        body: JSON.stringify(requestBody),
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const pageData = await response.json()

      // Extract products from the response
      let pageProducts = []
      if (pageData.results && pageData.results[0] && pageData.results[0].hits) {
        pageProducts = pageData.results[0].hits
        console.log(
          `Found ${pageProducts.length} products on ${
            categoryConfig.name
          } page ${page + 1}`
        )

        allProducts = allProducts.concat(pageProducts)
        console.log(
          `${categoryConfig.name} total products so far: ${allProducts.length}`
        )

        if (pageData.results[0].page == pageData.results[0].nbPages) {
          hasMoreProducts = false
        } else {
          page++
        }
      } else {
        console.log(
          `No products found on ${categoryConfig.name} page ${page + 1}`
        )
        hasMoreProducts = false
      }
    } catch (error) {
      console.error(
        `Error fetching ${categoryConfig.name} page ${page + 1}:`,
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
    const gender = product._gender || 'Women'
    const category = product._category || ''

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.name || product.title
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
  const cleanBrandName = 'aritzia'
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
      name: storeData.name || 'Aritzia',
      domain: 'aritzia.com',
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

async function scrapeAritziaProducts() {
  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Starting Aritzia scraping using Algolia API...')

    // Define categories to scrape - Aritzia category structure
    const categories = [
      {
        name: 'All Clothing',
        gender: 'Women',
        query: '',
        filters:
          'categories:clothing AND (orderable:true OR searchableIfUnavailable:true)',
        ruleContext: 'clothing',
      },
      {
        name: 'New Arrivals',
        gender: 'Women',
        query: '',
        filters:
          'categories:clothing AND (orderable:true OR searchableIfUnavailable:true) AND isNew:true',
        ruleContext: 'clothing',
      },
      {
        name: 'Sale',
        gender: 'Women',
        query: '',
        filters:
          'categories:clothing AND (orderable:true OR searchableIfUnavailable:true) AND isOnSale:true',
        ruleContext: 'clothing',
      },
      {
        name: 'Dresses',
        gender: 'Women',
        query: 'dresses',
        filters:
          'categories:clothing AND (orderable:true OR searchableIfUnavailable:true)',
        ruleContext: 'clothing',
      },
      {
        name: 'Tops',
        gender: 'Women',
        query: 'tops',
        filters:
          'categories:clothing AND (orderable:true OR searchableIfUnavailable:true)',
        ruleContext: 'clothing',
      },
    ]

    const targetProductsPerCategory = 500
    const allResults = []

    const storeData = {
      name: 'Aritzia',
      domain: 'aritzia.com',
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

      const categoryProducts = await scrapeAritziaCategory(
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
    console.log('🎉 ALL ARITZIA SCRAPING COMPLETED SUCCESSFULLY! 🎉')
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
    // Close browser, cleanup local images directory, and disconnect from MongoDB
    await closeBrowser()
    await cleanupLocalImagesDirectory()
    await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeAritziaProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 Aritzia products crawling completed successfully!')

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
      console.log('\n❌ Aritzia crawling failed')
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

module.exports = { main, scrapeAritziaProducts }
