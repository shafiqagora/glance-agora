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
  getDomainName,
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  retryRequestWithProxyRotation,
  createAxiosInstance,
} = require('./utils/helper')
const _ = require('lodash')

axios.defaults.timeout = 180000

// Function to fetch Lulu's stores from MongoDB
async function fetchLulusStoresFromServer(page = 1, limit = 1) {
  try {
    console.log(
      `🔍 Fetching Lulu's stores from MongoDB - Page ${page}, Limit ${limit}`
    )

    // Calculate skip value for pagination
    const skip = (page - 1) * limit

    // Fetch stores from MongoDB with pagination and populate products
    const stores = await Store.find({
      storeType: 'lulus',
    })
      .populate('products')
      .skip(skip)
      .limit(limit)
      .sort({ updatedAt: -1 }) // Sort by most recently updated first
      .lean() // Use lean() for better performance when we don't need mongoose documents

    // Get total count for pagination
    const totalStores = await Store.countDocuments({
      storeType: 'lulus',
      isScrapped: true,
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
      returnPolicy:
        store.returnPolicy ||
        'https://www.lulus.com/customerservice/article/returns-policy',
      storeType: store.storeType,
      isScrapped: store.isScrapped,
      products: store.products || [],
      createdAt: store.createdAt,
      updatedAt: store.updatedAt,
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

// Function to load products from lulusNewData.json file
function loadProductsFromFile() {
  try {
    const filePath = path.join(__dirname, 'lulusNewData.json')

    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`)
    }

    console.log(`📁 Loading products from: ${filePath}`)
    const fileContent = fs.readFileSync(filePath, 'utf8')
    const data = JSON.parse(fileContent)

    console.log(`✅ Loaded ${data.products?.length || 0} products from file`)
    return data.products || []
  } catch (error) {
    console.error('❌ Error loading products from file:', error.message)
    throw error
  }
}

// Enhanced generateCSV function with recrawl logic for Lulu's
async function generateCSVWithRecrawl(products, storeData, store) {
  const countryCode = storeData.country || 'US'
  const BATCH_SIZE = 50
  const allProductIds = []
  let allFormattedProducts = []

  products = _.uniqBy(products, (p) => p.parent_product_id)

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
        existingProductsMap.set(product.parent_product_id.toString(), product)
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

  console.log(
    `🔄 RECRAWL MODE: Found ${existingProducts.length} existing products in database`
  )
  console.log(
    `📦 Processing ${products.length} products from Lulu's in batches of ${BATCH_SIZE}...`
  )

  // Track current product IDs from file data
  const currentProductIds = new Set(
    products.map((p) => p.parent_product_id.toString())
  )
  const currentVariantIds = new Set()

  // First, handle products that exist in database but not in file data (DELETE)
  const deletedProducts = []
  for (const [productId, product] of existingProductsMap) {
    if (!currentProductIds.has(productId)) {
      // Product exists in DB but not in file data - mark as DELETE
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
    `🗑️  Found ${deletedProducts.length} products to delete (exist in DB but not in file data)`
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
      // Get existing product from database
      const existingProduct = existingProductsMap.get(
        product.parent_product_id.toString()
      )

      const formattedProduct = {
        parent_product_id: product.parent_product_id.toString(),
        name: product.name,
        description: product.description || '',
        category: product.category || '',
        retailer_domain: 'lulus.com',
        brand: 'Lulus',
        gender: product.gender || '',
        materials: product.materials || '',
        return_policy_link: storeData.returnPolicy,
        return_policy: product.return_policy || '',
        size_chart: product.size_chart || '',
        available_bank_offers: product.available_bank_offers || '',
        available_coupons: product.available_coupons || '',
        variants: [],
        operation_type: 'NO_CHANGE', // Will be determined based on variant operations
        source: 'lulus',
        _id:
          existingProduct && existingProduct._id
            ? existingProduct._id
            : new mongoose.Types.ObjectId(),
      }

      // Track variant operations for this product
      const variantOperations = []

      // Process variants from file data
      if (product.variants && Array.isArray(product.variants)) {
        for (const variant of product.variants) {
          const variantId = variant.variant_id
          currentVariantIds.add(variantId)

          const existingVariant = existingVariantsMap.get(variantId)

          const newVariantData = {
            variant_description: variant.variant_description || '',
            price_currency: variant.price_currency || 'USD',
            original_price: parseFloat(variant.original_price || 0),
            link_url: variant.link_url || '',
            deeplink_url: variant.deeplink_url || '',
            image_url: variant.image_url || '',
            alternate_image_urls: variant.alternate_image_urls || [],
            is_on_sale: variant.is_on_sale || false,
            is_in_stock: variant.is_in_stock || false,
            size: variant.size || '',
            color: variant.color || '',
            mpn: variant.mpn || '',
            ratings_count: parseInt(variant.ratings_count || 0),
            average_ratings: parseFloat(variant.average_ratings || 0),
            review_count: parseInt(variant.review_count || 0),
            selling_price: parseFloat(variant.selling_price || 0),
            sale_price: variant.sale_price
              ? parseFloat(variant.sale_price)
              : null,
            final_price: parseFloat(variant.final_price || 0),
            discount: parseFloat(variant.discount || 0),
            variant_id: variantId,
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
      }

      // Check for deleted variants (exist in DB but not in file data)
      if (existingProduct) {
        const currentVariantIdsForProduct = new Set()
        if (product.variants && Array.isArray(product.variants)) {
          product.variants.forEach((variant) => {
            currentVariantIdsForProduct.add(variant.variant_id)
          })
        }

        if (
          existingProduct.variants &&
          Array.isArray(existingProduct.variants)
        ) {
          existingProduct.variants.forEach((existingVariant) => {
            if (
              existingVariant &&
              !currentVariantIdsForProduct.has(existingVariant.variant_id)
            ) {
              // Variant exists in DB but not in file data - mark as DELETE
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

      formattedProduct.variants = _.uniqBy(
        formattedProduct.variants,
        (p) => p.variant_id
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
      'https://www.lulus.com',
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
    'https://www.lulus.com',
    countryCode
  )

  console.log(
    `\n📊 Recrawl Results: ${allProductIds.length} products processed`
  )

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
    const existingStore = await Store.findOne({
      storeType: 'lulus',
      name: 'Lulus',
    })

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
      name: storeData.name || 'Lulus',
      storeUrl: correctUrl,
      city: '',
      state: '',
      country: storeData.country || 'US',
      products: productIds.map((id) => new mongoose.Types.ObjectId(id)),
      isScrapped: true,
      storeType: 'lulus',
      returnPolicy:
        'https://www.lulus.com/customerservice/article/returns-policy',
    })

    await storeEntry.save()
    console.log(`✅ Created new store entry: ${storeData.name || 'Lulus'}`)
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error updating store entry for ${storeData.name || 'Lulus'}:`,
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
  // Create directory structure: countryCode/retailername-countryCode/
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

  // Remove MongoDB _id from output
  const cleanedProducts = allFormattedProducts.map((item) => {
    const { _id, ...itemWithoutId } = item
    return itemWithoutId
  })

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog-recrawl.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'Lulus',
      domain: 'lulus.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: cleanedProducts.length,
      crawled_at: new Date().toISOString(),
      crawl_type: 'RECRAWL',
    },
    products: cleanedProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
  const jsonlContent = cleanedProducts
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

  return { jsonFilePath, jsonlFilePath, gzippedFilePath }
}

// Enhanced main function for recrawling Lulu's
const main = async (returnPolicy = '', store) => {
  try {
    console.log(`🔄 Mode: Recrawl from file`)

    const storeData = {
      name: 'Lulus',
      domain: 'lulus.com',
      currency: 'USD',
      country: 'US',
      returnPolicy: returnPolicy,
    }

    console.log(`Starting to recrawl Lulu's from file...`)

    // Load products from the JSON file
    const products = loadProductsFromFile()

    if (products.length === 0) {
      console.log(`No products found in file`)
      return false
    }

    console.log(`Total products loaded from file: ${products.length}`)

    // Process with recrawl logic
    const result = await generateCSVWithRecrawl(products, storeData, store)

    console.log(`Successfully processed Lulu's`)
    console.log(`JSON saved at: ${result.jsonPath}`)

    return { result, storeData }
  } catch (e) {
    console.error("Error in recrawling Lulu's store:", e)
    return false
  }
}

// Process Lulu's stores fetched from server
async function processStoresFromServer() {
  console.log(`🚀 Starting to process Lulu's stores from server...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: 0,
    totalPages: 0,
  }

  // For now, create a default Lulu's store entry if none exists
  try {
    const existingStore = await Store.findOne({
      storeType: 'lulus',
      name: 'Lulus',
    })

    const returnPolicy =
      'https://www.lulus.com/customerservice/article/returns-policy'

    console.log("Processing Lulu's store...")
    const storeResult = await main(returnPolicy, existingStore)

    if (storeResult && !storeResult.skipped) {
      results.successful.push({
        brandName: storeResult.storeData.name,
        url: 'https://www.lulus.com',
        region: storeResult.storeData.country,
        jsonPath: storeResult.result.jsonPath,
      })
      console.log("✅ Successfully processed Lulu's")
    } else {
      results.failed.push({
        brandName: 'Lulus',
        url: 'https://www.lulus.com',
        error: 'Processing failed',
      })
      console.log("❌ Failed to process Lulu's")
    }
  } catch (error) {
    results.failed.push({
      brandName: 'Lulus',
      url: 'https://www.lulus.com',
      error: error.message,
    })
    console.log(`❌ Error processing Lulu's: ${error.message}`)
  }

  results.total = 1

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('LULUS RECRAWL PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores processed: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Failed: ${results.failed.length}`)

  // Save results to JSON file
  const resultsPath = path.join(
    __dirname,
    'lulus-recrawl-processing-results.json'
  )
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2))
  console.log(`\nDetailed results saved to: ${resultsPath}`)

  await disconnectDB()
  return results
}

// Export functions
module.exports = {
  main,
  processStoresFromServer,
  fetchLulusStoresFromServer,
  generateCSVWithRecrawl,
  compareVariants,
  compareProducts,
  loadProductsFromFile,
}

// If run directly from command line
if (require.main === module) {
  console.log("🔄 Processing Lulu's stores from server for RECRAWL...")
  processStoresFromServer()
    .then((results) => {
      console.log("\n🎉 Lulu's store processed!")
      if (results.failed.length > 0) {
        console.log(
          `⚠️  Failed to process store. Check lulus-recrawl-processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log("🎉 Lulu's store processed successfully!")
      }
    })
    .catch((error) => {
      console.error("Error processing Lulu's stores:", error)
      process.exit(1)
    })
}
