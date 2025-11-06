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
const mongoose = require('mongoose')
const {
  useDigitalOceanAI,
  cleanAndTruncate,
  fetchProductVariants,
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  retryRequestWithProxyRotation,
  createAxiosInstance,
  determineProductDetailsNYDJ,
  retryPuppeteerWithProxyRotation,
} = require('./utils/helper')
const _ = require('lodash')

axios.defaults.timeout = 180000

// Function to fetch stores from MongoDB
async function fetchStoresFromServer(page = 1, limit = 2) {
  try {
    console.log(
      `🔍 Fetching stores from MongoDB - Page ${page}, Limit ${limit}`
    )

    // Calculate skip value for pagination
    const skip = (page - 1) * limit

    // Fetch stores from MongoDB with pagination and populate products
    const stores = await Store.find({
      storeType: 'shopify',
      name: {
        $nin: ['Kith', 'Mack Weldon', 'Relwen'],
      }, // Skip stores with these names
    })
      .populate('products')
      .skip(skip)
      .limit(limit)
      .sort({ updatedAt: -1 }) // Sort by most recently updated first
      .lean() // Use lean() for better performance when we don't need mongoose documents

    // Get total count for pagination - fix: use same filter as the main query
    const totalStores = await Store.countDocuments({
      storeType: 'shopify',
      name: { $nin: ['Kith', 'Mack Weldon', 'Relwen'] }, // Skip stores with these names
    })

    const totalPages = Math.ceil(totalStores / limit)

    console.log(
      `✅ Fetched ${stores.length} stores from MongoDB (${totalStores} total)`
    )

    // Transform MongoDB stores to match expected format
    const transformedStores = stores.map((store) => ({
      _id: store._id,
      name: store.name,
      brandName: store.name,
      storeUrl: store.storeUrl,
      url: store.storeUrl,
      city: store.city,
      state: store.state,
      country: store.country,
      returnPolicy: '', // Will be fetched from store metadata during crawl
      storeType: store.storeType,
      isScrapped: store.isScrapped,
      products: store.products || [],
      createdAt: store.createdAt,
      updatedAt: store.updatedAt,
      returnPolicy: store.returnPolicy,
    }))

    return {
      stores: transformedStores,
      totalPages,
      currentPage: page,
      totalStores,
    }
  } catch (error) {
    console.log(error, 'error is')
    return { stores: [], totalPages: 0, currentPage: 0, totalStores: 0 }
  }
}

// Function to compare two variants and determine operation type
function compareVariants(existingVariant, newVariant) {
  if (!existingVariant) return 'INSERT'

  // Compare key fields to determine if variant has changed
  const fieldsToCompare = [
    'original_price',
    'selling_price',
    'sale_price',
    'final_price',
    'is_on_sale',
    'is_in_stock',
    'image_url',
    'link_url',
    'deeplink_url',
  ]

  for (const field of fieldsToCompare) {
    if (existingVariant[field] !== newVariant[field]) {
      return 'UPDATE'
    }
  }

  return 'NO_CHANGE'
}

// Function to compare two products and determine operation type
function compareProducts(existingProduct, newProduct) {
  if (!existingProduct) return 'INSERT'

  // Compare key fields to determine if product has changed
  const fieldsToCompare = ['name', 'description', 'brand', 'category']

  for (const field of fieldsToCompare) {
    if (existingProduct[field] !== newProduct[field]) {
      return 'UPDATE'
    }
  }

  return 'NO_CHANGE'
}

// Function to determine product operation type based on variant operations
function determineProductOperationType(existingProduct, variantOperations) {
  // If product doesn't exist in DB, it's INSERT
  if (!existingProduct) {
    return 'INSERT'
  }

  // Check variant operations to determine product operation
  const hasInsertVariants = variantOperations.some((op) => op === 'INSERT')
  const hasUpdateVariants = variantOperations.some((op) => op === 'UPDATE')
  const hasDeleteVariants = variantOperations.some((op) => op === 'DELETE')

  // If any variant has changes, product is UPDATE
  if (hasInsertVariants || hasUpdateVariants || hasDeleteVariants) {
    return 'UPDATE'
  }

  // If all variants are NO_CHANGE, product is NO_CHANGE
  return 'NO_CHANGE'
}

// Enhanced generateCSV function with recrawl logic
async function generateCSVWithRecrawl(products, storeData, correctUrl, store) {
  const countryCode = storeData.country
  const BATCH_SIZE = 5000
  const allProductIds = []
  let allFormattedProducts = []

  products = _.uniqBy(products, 'id')

  // Ensure store object exists
  if (!store) {
    console.log('⚠️  Store object is undefined, treating as new store')
    store = { products: [] }
  }

  // Get existing products for this store from database
  const existingProducts = store.products || []

  // Create maps for quick lookup
  const existingProductsMap = new Map()
  const existingVariantsMap = new Map()

  if (existingProducts && existingProducts.length > 0) {
    existingProducts.forEach((product) => {
      if (product && product.parent_product_id) {
        existingProductsMap.set(product.parent_product_id, product)
        if (product.variants && Array.isArray(product.variants)) {
          product.variants.forEach((variant) => {
            if (variant && variant.variant_id) {
              existingVariantsMap.set(variant.variant_id, variant)
            }
          })
        }
      }
    })
  }

  console.log('existing product maps', store.name)
  console.log('Store products type:', typeof store.products)
  console.log(
    'Store products length:',
    store.products ? store.products.length : 'undefined'
  )

  console.log(
    `🔄 RECRAWL MODE: Found ${existingProducts.length} existing products in database`
  )
  console.log(
    `📦 Processing ${products.length} products from Shopify in batches of ${BATCH_SIZE}...`
  )

  // Track current product IDs from Shopify
  const currentProductIds = new Set(products.map((p) => p.id.toString()))
  const currentVariantIds = new Set()

  // First, handle products that exist in database but not on store (DELETE)
  const deletedProducts = []
  for (const [productId, product] of existingProductsMap) {
    if (!currentProductIds.has(productId)) {
      // Product exists in DB but not on store - mark as DELETE
      const deletedProduct = {
        ...product,
        operation_type: 'DELETE',
        variants: product.variants
          ? product.variants.map((variant) => ({
              ...variant,
              operation_type: 'DELETE',
            }))
          : [],
      }
      deletedProducts.push(deletedProduct)
      allFormattedProducts.push(deletedProduct)
      if (product._id) {
        allProductIds.push(product._id.toString())
      }
    }
  }

  console.log(
    `🗑️  Found ${deletedProducts.length} products to delete (exist in DB but not on store)`
  )

  // Split products into batches
  for (let i = 0; i < products.length; i += BATCH_SIZE) {
    const batch = products.slice(i, i + BATCH_SIZE)
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1
    const totalBatches = Math.ceil(products.length / BATCH_SIZE)

    console.log(
      `\n📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} products)...`
    )

    const formattedProducts = []

    // Format products in current batch
    for (const product of batch) {
      const productUrl = `${correctUrl}/products/${product.handle}`
      const variants = product.variants

      // Get existing product from database
      const existingProduct = existingProductsMap.get(product.id.toString())

      const formattedProduct = {
        parent_product_id: product.id.toString(),
        name: product.title,
        description: sanitizeHtml(product.body_html || '', {
          allowedTags: [],
          allowedAttributes: {},
        })
          .replace(/&amp;/g, '&')
          .replace(/&lt;/g, '<')
          .replace(/&gt;/g, '>')
          .replace(/&quot;/g, '"')
          .replace(/&#39;/g, "'")
          .replace(/&nbsp;/g, ' ')
          .replace(/[\u0000-\u001F\u007F-\u009F]/g, '')
          .replace(/[^\x00-\x7F]/g, '')
          .replace(/\s+/g, ' ')
          .trim(),
        category: '',
        retailer_domain: getDomainName(correctUrl),
        brand: product.vendor,
        gender: '',
        materials: '',
        return_policy_link: storeData.returnPolicy,
        return_policy: '',
        size_chart: '',
        available_bank_offers: '',
        available_coupons: '',
        variants: [],
        operation_type: 'NO_CHANGE', // Will be determined based on variant operations
        source: 'shopify',
        _id:
          existingProduct && existingProduct._id
            ? existingProduct._id
            : new mongoose.Types.ObjectId(),
      }

      // Track variant operations for this product
      const variantOperations = []

      // Process variants from Shopify
      for (const variant of variants) {
        currentVariantIds.add(variant.id.toString())

        const originalPrice = parseFloat(
          variant.compare_at_price && variant.compare_at_price > 0
            ? variant.compare_at_price
            : variant.price
        )
        const sellingPrice = parseFloat(variant.price || 0)
        const salePrice =
          variant.compare_at_price && variant.compare_at_price > sellingPrice
            ? sellingPrice
            : null
        const finalPrice = sellingPrice
        const discount = calculateDiscount(originalPrice, finalPrice)
        const isOnSale =
          variant.compare_at_price && variant.compare_at_price > sellingPrice
            ? true
            : false
        const isInStock = variant.available || false

        // Get variant image or fallback to product images
        let imageUrl = ''
        let alternateImages = []

        if (variant.featured_image && variant.featured_image.src) {
          imageUrl = variant.featured_image.src
        } else if (product.images && product.images.length > 0) {
          imageUrl = product.images[0].src
        }

        // Collect alternate images
        if (product.images && product.images.length > 1) {
          alternateImages = product.images
            .slice(1)
            .map((img) => img.src)
            .filter((src) => src !== imageUrl)
        }

        const extractedSize = extractSize(variant, product.options)
        const extractedColor = extractColor(variant, product.options)
        const existingVariant = existingVariantsMap.get(variant.id.toString())

        const newVariantData = {
          variant_description: '',
          price_currency: storeData.currency,
          original_price: originalPrice,
          link_url: `${productUrl}?variant=${variant.id}`,
          deeplink_url: `${productUrl}?variant=${variant.id}`,
          image_url: imageUrl,
          alternate_image_urls: alternateImages,
          is_on_sale: isOnSale,
          is_in_stock: isInStock,
          size: extractedSize,
          color: extractedColor,
          mpn: uuidv5(
            `${product.id}-${extractedColor || 'NO_COLOR'}`,
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
          ),
          ratings_count: 0,
          average_ratings: 0,
          review_count: 0,
          selling_price: sellingPrice,
          sale_price: salePrice,
          final_price: finalPrice,
          discount: discount,
          variant_id: variant.id.toString(),
        }

        const variantOperationType = compareVariants(
          existingVariant,
          newVariantData
        )
        variantOperations.push(variantOperationType)

        const formattedVariant = {
          ...newVariantData,
          operation_type: variantOperationType,
        }

        formattedProduct.variants.push(formattedVariant)
      }

      // Check for deleted variants (exist in DB but not on Shopify)
      if (existingProduct) {
        const currentVariantIdsForProduct = new Set(
          variants.map((v) => v.id.toString())
        )

        if (
          existingProduct.variants &&
          Array.isArray(existingProduct.variants)
        ) {
          existingProduct.variants.forEach((existingVariant) => {
            if (
              existingVariant &&
              !currentVariantIdsForProduct.has(existingVariant.variant_id)
            ) {
              // Variant exists in DB but not on Shopify - mark as DELETE
              const deletedVariant = {
                ...existingVariant,
                operation_type: 'DELETE',
              }
              formattedProduct.variants.push(deletedVariant)
              variantOperations.push('DELETE')
            }
          })
        }
      }

      // Determine product operation type based on variant operations
      formattedProduct.operation_type = determineProductOperationType(
        existingProduct,
        variantOperations
      )

      formattedProducts.push(formattedProduct)
    }

    // Handle database operations for current batch
    let batchProductIds = []
    if (formattedProducts.length > 0) {
      try {
        const operationResults = await processBatchWithOperations(
          formattedProducts
        )
        batchProductIds = operationResults.map((result) => result.productId)

        console.log(`✅ Batch ${batchNumber} operations completed:`)
        console.log(
          `   - INSERT: ${
            operationResults.filter((r) => r.operation === 'INSERT').length
          }`
        )
        console.log(
          `   - UPDATE: ${
            operationResults.filter((r) => r.operation === 'UPDATE').length
          }`
        )
        console.log(
          `   - DELETE: ${
            operationResults.filter((r) => r.operation === 'DELETE').length
          }`
        )
        console.log(
          `   - NO_CHANGE: ${
            operationResults.filter((r) => r.operation === 'NO_CHANGE').length
          }`
        )

        // Add to overall collections
        allProductIds.push(...batchProductIds)
        allFormattedProducts.push(...formattedProducts)
      } catch (error) {
        console.error(
          `❌ Error processing batch ${batchNumber}:`,
          error.message
        )
        throw error
      }
    }

    // Add small delay between batches
    if (batchNumber < totalBatches) {
      console.log('Waiting 1 second before processing next batch...')
      await new Promise((resolve) => setTimeout(resolve, 1000))
    }
  }

  // Generate operation summary
  const operationSummary = {
    products: { INSERT: 0, UPDATE: 0, DELETE: 0, NO_CHANGE: 0 },
    variants: { INSERT: 0, UPDATE: 0, DELETE: 0, NO_CHANGE: 0 },
  }

  allFormattedProducts.forEach((product) => {
    operationSummary.products[product.operation_type]++
    product.variants.forEach((variant) => {
      operationSummary.variants[variant.operation_type]++
    })
  })

  console.log('\n📊 Operation Summary:')
  console.log(
    `Products - INSERT: ${operationSummary.products.INSERT}, UPDATE: ${operationSummary.products.UPDATE}, DELETE: ${operationSummary.products.DELETE}, NO_CHANGE: ${operationSummary.products.NO_CHANGE}`
  )
  console.log(
    `Variants - INSERT: ${operationSummary.variants.INSERT}, UPDATE: ${operationSummary.variants.UPDATE}, DELETE: ${operationSummary.variants.DELETE}, NO_CHANGE: ${operationSummary.variants.NO_CHANGE}`
  )

  console.log(
    `\n✅ All batches processed! Total: ${allFormattedProducts.length} products`
  )

  // Update store entry
  try {
    const storeResult = await updateStoreEntry(
      storeData,
      correctUrl,
      allProductIds
    )
    console.log(`✅ Store entry updated: ${storeResult.name}`)
  } catch (error) {
    console.error(`❌ Error updating store entry:`, error.message)
  }

  // Generate output files
  const outputResult = await generateOutputFiles(
    allFormattedProducts,
    storeData,
    correctUrl,
    countryCode
  )

  console.log(
    `\n📊 Recrawl Results: ${allProductIds.length} products processed`
  )

  console.log(outputResult)

  return { jsonPath: outputResult.gzippedFilePath, productIds: allProductIds }
}

// Function to process batch with proper operations
async function processBatchWithOperations(formattedProducts) {
  const results = []

  for (const product of formattedProducts) {
    try {
      let result

      if (product.operation_type === 'INSERT') {
        // Insert new product
        const newProduct = new Product(product)
        await newProduct.save()
        result = { productId: newProduct._id.toString(), operation: 'INSERT' }
      } else if (product.operation_type === 'UPDATE') {
        // Update existing product
        const updateData = { ...product }
        delete updateData._id
        await Product.findByIdAndUpdate(product._id, updateData)
        result = { productId: product._id.toString(), operation: 'UPDATE' }
      } else if (product.operation_type === 'DELETE') {
        // Mark product as deleted (we keep it in DB for audit trail)
        await Product.findByIdAndUpdate(product._id, {
          operation_type: 'DELETE',
          'variants.$[].operation_type': 'DELETE',
        })
        result = { productId: product._id.toString(), operation: 'DELETE' }
      } else {
        // NO_CHANGE - just track the ID
        result = { productId: product._id.toString(), operation: 'NO_CHANGE' }
      }

      results.push(result)
    } catch (error) {
      console.error(
        `❌ Error processing product ${product.name}:`,
        error.message
      )
      throw error
    }
  }

  return results
}

// Function to update store entry
async function updateStoreEntry(storeData, correctUrl, productIds) {
  try {
    const existingStore = await Store.findOne({ storeUrl: correctUrl })

    if (existingStore) {
      console.log(`Store ${storeData.name} already exists, updating...`)

      // Update store with new timestamp and ensure all product IDs are included
      const allProductIds = [
        ...new Set([
          ...existingStore.products.map((id) => id.toString()),
          ...productIds,
        ]),
      ]
      existingStore.products = allProductIds.map(
        (id) => new mongoose.Types.ObjectId(id)
      )
      existingStore.isScrapped = true
      existingStore.updatedAt = new Date()
      await existingStore.save()

      console.log(`Updated store with ${allProductIds.length} total products`)
      return existingStore
    }

    // Create new store entry
    const storeEntry = new Store({
      name: storeData.name,
      storeUrl: correctUrl,
      city: storeData.city,
      state: storeData.province || storeData.state,
      country: storeData.country,
      products: productIds.map((id) => new mongoose.Types.ObjectId(id)),
      isScrapped: true,
      storeType: 'shopify',
    })

    await storeEntry.save()
    console.log(`✅ Created new store entry: ${storeData.name}`)
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error updating store entry for ${storeData.name}:`,
      error.message
    )
    throw error
  }
}

// Function to generate output files
async function generateOutputFiles(
  allFormattedProducts,
  storeData,
  correctUrl,
  countryCode
) {
  try {
    // Create directory structure: countryCode/retailername-countryCode/
    const cleanBrandName = (storeData.name || getDomainName(correctUrl))
      .replace(/[^a-zA-Z0-9.-]/g, '-')
      .toLowerCase()
    const dirPath = path.join(
      __dirname,
      'output',
      countryCode,
      `${cleanBrandName}-${countryCode}`
    )

    // Create directory if it doesn't exist
    try {
      if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
      }
    } catch (dirError) {
      console.error(`❌ Error creating directory ${dirPath}:`, dirError.message)
      throw new Error(`Failed to create output directory: ${dirError.message}`)
    }

    // Remove MongoDB _id from output
    let cleanedProducts
    try {
      cleanedProducts = allFormattedProducts.map((item) => {
        const { _id, ...itemWithoutId } = item
        return itemWithoutId
      })
    } catch (cleanError) {
      console.error('❌ Error cleaning products data:', cleanError.message)
      throw new Error(`Failed to clean products data: ${cleanError.message}`)
    }

    // Prepare catalog data
    let catalogData
    try {
      catalogData = {
        store_info: {
          name: storeData.name || getDomainName(correctUrl),
          domain: getDomainName(correctUrl),
          currency: storeData.currency || 'USD',
          country: countryCode,
          total_products: cleanedProducts.length,
          crawled_at: new Date().toISOString(),
          crawl_type: 'RECRAWL',
        },
        products: cleanedProducts,
      }
    } catch (dataError) {
      console.error('❌ Error preparing catalog data:', dataError.message)
      throw new Error(`Failed to prepare catalog data: ${dataError.message}`)
    }

    // Save formatted data as JSON
    const jsonFilePath = path.join(dirPath, 'catalog-recrawl.json')
    try {
      fs.writeFileSync(
        jsonFilePath,
        JSON.stringify(catalogData, null, 2),
        'utf8'
      )
      console.log(`✅ JSON file generated: ${jsonFilePath}`)
    } catch (jsonError) {
      console.error(
        `❌ Error writing JSON file ${jsonFilePath}:`,
        jsonError.message
      )
      throw new Error(`Failed to write JSON file: ${jsonError.message}`)
    }

    // Create JSONL file (each product on a separate line)
    const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
    try {
      const jsonlContent = cleanedProducts
        .map((product) => JSON.stringify(product))
        .join('\n')
      fs.writeFileSync(jsonlFilePath, jsonlContent, 'utf8')
      console.log(`✅ JSONL file generated: ${jsonlFilePath}`)
    } catch (jsonlError) {
      console.error(
        `❌ Error writing JSONL file ${jsonlFilePath}:`,
        jsonlError.message
      )
      throw new Error(`Failed to write JSONL file: ${jsonlError.message}`)
    }

    // Gzip the JSONL file
    const gzippedFilePath = `${jsonlFilePath}.gz`
    try {
      const jsonlBuffer = fs.readFileSync(jsonlFilePath)
      const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
      fs.writeFileSync(gzippedFilePath, gzippedBuffer)
      console.log(`✅ Gzipped JSONL file generated: ${gzippedFilePath}`)
    } catch (gzipError) {
      console.error(
        `❌ Error creating gzipped file ${gzippedFilePath}:`,
        gzipError.message
      )
      throw new Error(`Failed to create gzipped file: ${gzipError.message}`)
    }

    return { jsonFilePath, jsonlFilePath, gzippedFilePath }
  } catch (error) {
    console.error('❌ Error in generateOutputFiles:', error.message)
    throw error
  }
}

// Enhanced main function for recrawling
const main = async (SITE_URL, returnPolicy = '', store) => {
  try {
    // Find the correct URL format
    let correctUrl = SITE_URL
    if (!correctUrl) {
      console.log(`❌ Could not find working URL format for ${SITE_URL}`)
      return false
    }

    let DOMAIN = getDomainName(correctUrl)
    console.log(`✅ Using URL: ${correctUrl}`)
    console.log(`🔄 Mode: Recrawl`)

    let allProducts = []
    let loopCondition = true
    let pageCounter = 1
    let storeData = {}

    try {
      const metaData = await retryPuppeteerWithProxyRotation(
        async (browser) => {
          const page = await browser.newPage()

          await page.setUserAgent(
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
          )

          await page.setExtraHTTPHeaders({
            Accept: 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
          })

          const response = await page.goto(`${correctUrl}/meta.json`, {
            waitUntil: 'networkidle2',
            timeout: 60000,
          })

          const responseBody = await response.text()
          return JSON.parse(responseBody)
        },
        2,
        2000,
        store?.country || 'US', // Use store country from database or default to US,
        correctUrl
      )

      storeData = {
        ...metaData,
        region: metaData.country,
        returnPolicy,
        url: correctUrl,
      }
    } catch (err) {
      console.log('Could not fetch store metadata, using defaults')
      return false
    }

    console.log(`Starting to recrawl ${storeData.name || DOMAIN}...`)

    // Fetch all products
    do {
      try {
        const productsData = await retryPuppeteerWithProxyRotation(
          async (browser) => {
            const page = await browser.newPage()

            await page.setUserAgent(
              'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            await page.setExtraHTTPHeaders({
              Accept: 'application/json, text/plain, */*',
              'Accept-Language': 'en-US,en;q=0.9',
              'Accept-Encoding': 'gzip, deflate, br',
            })

            const response = await page.goto(
              `${correctUrl}/products.json?page=${pageCounter}&limit=250`,
              {
                waitUntil: 'networkidle2',
                timeout: 60000,
              }
            )

            const responseBody = await response.text()
            return JSON.parse(responseBody)
          },
          2,
          2000,
          storeData.country || store?.country || 'US', // Use store country or default to US
          correctUrl
        )

        const products = productsData?.products

        if (!products || products.length === 0) {
          console.log(`Ending at page ${pageCounter}`)
          loopCondition = false
          break
        } else {
          allProducts.push(...products)
          console.log(
            `Fetched ${products.length} products from page ${pageCounter}`
          )
        }
      } catch (err) {
        console.log(`Error fetching page ${pageCounter}:`, err.message)

        if (
          err.message &&
          (err.message.includes('404') || err.message.includes('Not Found'))
        ) {
          console.log('Reached end of products (404)')
          loopCondition = false
          break
        }
      }

      pageCounter += 1
    } while (loopCondition)

    if (allProducts.length === 0) {
      console.log(`No products found for ${SITE_URL}`)
      return false
    }

    console.log(`Total products found: ${allProducts.length}`)

    // Process with recrawl logic
    const result = await generateCSVWithRecrawl(
      allProducts,
      storeData,
      correctUrl,
      store
    )

    console.log(`Successfully processed ${storeData.name || DOMAIN}`)
    console.log(`JSON saved at: ${result.jsonPath}`)

    return { result, storeData }
  } catch (e) {
    console.error('Error in recrawling Shopify store:', e)
    return false
  }
}

// Process stores fetched from server in batches of 25
async function processStoresFromServer() {
  console.log(`🚀 Starting to process stores from server (25 at a time)...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: 0,
    totalPages: 0,
    operations_summary: {
      total_inserts: 0,
      total_updates: 0,
      total_deletes: 0,
      total_no_changes: 0,
    },
  }

  let currentPage = 1
  let hasMorePages = true

  while (hasMorePages) {
    try {
      const serverResponse = await fetchStoresFromServer(currentPage, 1)

      if (serverResponse.stores.length === 0) {
        console.log('📄 No more stores to process')
        break
      }

      results.total += serverResponse.stores.length
      results.totalPages = serverResponse.totalPages

      console.log(
        `\n📄 Processing page ${currentPage}/${serverResponse.totalPages}`
      )
      console.log(`📦 Processing ${serverResponse.stores.length} stores...`)

      for (let i = 0; i < serverResponse.stores.length; i++) {
        const store = serverResponse.stores[i]
        console.log(
          `\n[${i + 1}/${serverResponse.stores.length}] Processing ${
            store.name || store.brandName
          } (${store.storeUrl || store.url})...`
        )

        try {
          const storeUrl = store.storeUrl || store.url
          const returnPolicy = store.returnPolicy || ''

          const storeResult = await main(storeUrl, returnPolicy, store) // true for recrawl mode

          if (storeResult && !storeResult.skipped) {
            results.successful.push({
              brandName: storeResult.storeData.name,
              url: storeResult.storeData.url,
              region: storeResult.storeData.region,
              jsonPath: storeResult.result.jsonPath,
            })
            console.log(
              `✅ Successfully processed ${store.name || store.brandName}`
            )
          } else if (storeResult && storeResult.skipped) {
            results.skipped.push({
              brandName: store.name || store.brandName,
              url: storeUrl,
              reason: storeResult.reason,
            })
            console.log(
              `⏭️  Skipped ${store.name || store.brandName}: ${
                storeResult.reason
              }`
            )
          } else {
            results.failed.push({
              brandName: store.name || store.brandName,
              url: storeUrl,
              error: 'Processing failed',
            })
            console.log(`❌ Failed to process ${store.name || store.brandName}`)
          }
        } catch (error) {
          results.failed.push({
            brandName: store.name || store.brandName,
            url: store.storeUrl || store.url,
            error: error.message,
          })
          console.log(
            `❌ Error processing ${store.name || store.brandName}: ${
              error.message
            }`
          )
        }

        // Add a small delay between requests to be respectful
        await new Promise((resolve) => setTimeout(resolve, 2000))
      }

      // Check if there are more pages
      hasMorePages = currentPage < serverResponse.totalPages
      currentPage++

      // Add delay between pages
      if (hasMorePages) {
        console.log('\n⏳ Waiting 5 seconds before processing next page...')
        await new Promise((resolve) => setTimeout(resolve, 5000))
      }
    } catch (error) {
      console.error(`❌ Error processing page ${currentPage}:`, error.message)
      break
    }
  }

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('RECRAWL PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores processed: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Skipped: ${results.skipped.length}`)
  console.log(`Failed: ${results.failed.length}`)

  if (results.successful.length > 0) {
    console.log('\n✅ Successfully processed stores:')
    results.successful.forEach((store) => {
      console.log(`  - ${store.brandName} (${store.region}):`)
      console.log(`    JSON: ${store.jsonPath}`)
    })
  }

  if (results.failed.length > 0) {
    console.log('\n❌ Failed stores:')
    results.failed.forEach((store) => {
      console.log(`  - ${store.brandName}: ${store.error}`)
    })
  }

  // Save results to JSON file
  const resultsPath = path.join(__dirname, 'recrawl-processing-results.json')
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2))
  console.log(`\nDetailed results saved to: ${resultsPath}`)

  await disconnectDB()
  return results
}

// Export functions
module.exports = {
  main,
  processStoresFromServer,
  fetchStoresFromServer,
  generateCSVWithRecrawl,
  compareVariants,
  compareProducts,
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  fetchProductVariants,
  cleanAndTruncate,
}

// If run directly from command line
if (require.main === module) {
  console.log('🔄 Processing stores from server for RECRAWL...')
  processStoresFromServer()
    .then((results) => {
      console.log('\n🎉 All stores processed!')
      if (results.failed.length > 0) {
        console.log(
          `⚠️  ${results.failed.length} stores failed. Check recrawl-processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log('🎉 All stores processed successfully!')
      }
    })
    .catch((error) => {
      console.error('Error processing stores:', error)
      process.exit(1)
    })
}
