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
const { filterValidProducts } = require('./validate-catalog')

axios.defaults.timeout = 180000

// Function to fetch Good American stores from MongoDB
async function fetchGoodAmericanStoresFromServer(page = 1, limit = 1) {
  try {
    console.log(
      `🔍 Fetching Good American stores from MongoDB - Page ${page}, Limit ${limit}`
    )

    // Calculate skip value for pagination
    const skip = (page - 1) * limit

    // Fetch stores from MongoDB with pagination and populate products
    const stores = await Store.find({
      storeType: 'good_american',
    })
      .populate('products')
      .skip(skip)
      .limit(limit)
      .sort({ updatedAt: -1 }) // Sort by most recently updated first
      .lean() // Use lean() for better performance when we don't need mongoose documents

    // Get total count for pagination
    const totalStores = await Store.countDocuments({
      storeType: 'good_american',
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
        'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU',
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

// Enhanced generateCSV function with recrawl logic for Good American
async function generateCSVWithRecrawl(products, storeData, store) {
  const countryCode = storeData.country || 'US'
  const BATCH_SIZE = 50
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

  console.log(
    `🔄 RECRAWL MODE: Found ${existingProducts.length} existing products in database`
  )
  console.log(
    `📦 Processing ${products.length} products from Good American in batches of ${BATCH_SIZE}...`
  )

  // Track current product IDs from Good American
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
      const productUrl = `https://www.goodamerican.com/en-pk/products/${product.handle}`
      const productId = product.id

      // Get existing product from database
      const existingProduct = existingProductsMap.get(productId.toString())

      // Get detailed product information
      const currentProduct = await getProductDetails(product.handle)

      const formattedProduct = {
        parent_product_id: productId.toString(),
        name: product.name,
        description: currentProduct?.product?.description || '',
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
        operation_type: 'NO_CHANGE', // Will be determined based on variant operations
        source: 'good_american',
        _id:
          existingProduct && existingProduct._id
            ? existingProduct._id
            : new mongoose.Types.ObjectId(),
      }

      // Track variant operations for this product
      const variantOperations = []

      // Process variants from Good American
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

          // Extract size and color from variant
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

          const variantId = variant.id?.toString().split('/').pop()
          currentVariantIds.add(variantId)

          const existingVariant = existingVariantsMap.get(variantId)

          const newVariantData = {
            variant_description: '',
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
              `${productId}-${color}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
            ),
            ratings_count: 0,
            average_ratings: 0,
            review_count: 0,
            selling_price: sellingPrice,
            sale_price: salePrice,
            final_price: finalPrice,
            discount: discount,
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

      // Check for deleted variants (exist in DB but not on Good American)
      if (existingProduct) {
        const currentVariantIdsForProduct = new Set()
        if (
          product?.variants &&
          product?.variants.nodes &&
          product?.variants.nodes.length > 0
        ) {
          for (const variant of product.variants.nodes) {
            const variantId = variant.id?.toString().split('/').pop()
            currentVariantIdsForProduct.add(variantId)
          }
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
              // Variant exists in DB but not on Good American - mark as DELETE
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
      'https://www.goodamerican.com',
      allProductIds
    )
    console.log(`✅ Store entry updated: ${storeResult.name}`)
  } catch (error) {
    console.error(`❌ Error updating store entry:`, error.message)
  }

  // Filter out invalid products using validation
  console.log(`\n🔍 Filtering products for validation...`)
  console.log(`📦 Products before filtering: ${allFormattedProducts.length}`)

  const filterResult = filterValidProducts(allFormattedProducts)
  const validProducts = filterResult.validProducts

  console.log(`✅ Valid products: ${filterResult.validCount}`)
  console.log(`❌ Invalid products filtered out: ${filterResult.invalidCount}`)
  console.log(
    `🔄 Total variants filtered: ${filterResult.totalVariantsFiltered}`
  )

  // Generate output files with filtered products
  const outputResult = await generateOutputFiles(
    validProducts,
    storeData,
    'https://www.goodamerican.com',
    countryCode
  )

  console.log(
    `\n📊 Recrawl Results: ${allProductIds.length} products processed, ${filterResult.validCount} valid products saved`
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
      storeType: 'good_american',
      name: 'Good American',
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
      name: storeData.name || 'Good American',
      storeUrl: correctUrl,
      city: '',
      state: '',
      country: storeData.country || 'US',
      products: productIds.map((id) => new mongoose.Types.ObjectId(id)),
      isScrapped: true,
      storeType: 'good_american',
      returnPolicy:
        'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU',
    })

    await storeEntry.save()
    console.log(
      `✅ Created new store entry: ${storeData.name || 'Good American'}`
    )
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error updating store entry for ${storeData.name || 'Good American'}:`,
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
  const cleanBrandName = 'good_american'
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
      name: storeData.name || 'Good American',
      domain: 'goodamerican.com',
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

// Enhanced main function for recrawling Good American
const main = async (API_URL, returnPolicy = '', store) => {
  try {
    console.log(`✅ Using URL: ${API_URL}`)
    console.log(`🔄 Mode: Recrawl`)

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

    console.log(`Starting to recrawl Good American...`)

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

    // Process with recrawl logic
    const result = await generateCSVWithRecrawl(allProducts, storeData, store)

    console.log(`Successfully processed Good American`)
    console.log(`JSON saved at: ${result.jsonPath}`)

    return { result, storeData }
  } catch (e) {
    console.error('Error in recrawling Good American store:', e)
    return false
  }
}

// Process Good American stores fetched from server
async function processStoresFromServer() {
  console.log(`🚀 Starting to process Good American stores from server...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: 0,
    totalPages: 0,
  }

  // For now, create a default Good American store entry if none exists
  try {
    const existingStore = await Store.findOne({
      storeType: 'good_american',
      name: 'Good American',
    })

    const API_URL = 'https://www.goodamerican.com/en-US/api/searchspring'
    const returnPolicy =
      'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU'

    console.log('Processing Good American store...')
    const storeResult = await main(API_URL, returnPolicy, existingStore)

    if (storeResult && !storeResult.skipped) {
      results.successful.push({
        brandName: storeResult.storeData.name,
        url: 'https://www.goodamerican.com',
        region: storeResult.storeData.country,
        jsonPath: storeResult.result.jsonPath,
      })
      console.log('✅ Successfully processed Good American')
    } else {
      results.failed.push({
        brandName: 'Good American',
        url: 'https://www.goodamerican.com',
        error: 'Processing failed',
      })
      console.log('❌ Failed to process Good American')
    }
  } catch (error) {
    results.failed.push({
      brandName: 'Good American',
      url: 'https://www.goodamerican.com',
      error: error.message,
    })
    console.log(`❌ Error processing Good American: ${error.message}`)
  }

  results.total = 1

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('GOOD AMERICAN RECRAWL PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores processed: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Failed: ${results.failed.length}`)

  // Save results to JSON file
  const resultsPath = path.join(
    __dirname,
    'good-american-recrawl-processing-results.json'
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
  fetchGoodAmericanStoresFromServer,
  generateCSVWithRecrawl,
  compareVariants,
  compareProducts,
  getProductDetails,
}

// If run directly from command line
if (require.main === module) {
  console.log('🔄 Processing Good American stores from server for RECRAWL...')
  processStoresFromServer()
    .then((results) => {
      console.log('\n🎉 Good American store processed!')
      if (results.failed.length > 0) {
        console.log(
          `⚠️  Failed to process store. Check good-american-recrawl-processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log('🎉 Good American store processed successfully!')
      }
    })
    .catch((error) => {
      console.error('Error processing Good American stores:', error)
      process.exit(1)
    })
}
