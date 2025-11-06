require('dotenv').config()
const axios = require('axios')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const fs = require('fs')
const zlib = require('zlib')
const path = require('path')
const readline = require('readline')
const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const Store = require('./models/Store')
const mongoose = require('mongoose')
const {
  calculateDiscount,
  extractSize,
  extractColor,
  determineProductDetails,
  cleanAndTruncate,
  getDomainName,
  retryRequestWithProxyRotation,
  createAxiosInstance,
} = require('./utils/helper')
const _ = require('lodash')
const { filterValidProducts } = require('./validate-catalog')

// Function to prompt for new authentication credentials
async function promptForNewCredentials() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  })

  return new Promise((resolve) => {
    console.log(
      '\n🔐 Authentication token has expired. Please provide new credentials:'
    )
    console.log(
      'You can get these from the browser developer tools Network tab when making a request to FreePeople API.'
    )

    rl.question(
      'Enter new Authorization header (Bearer token): ',
      (authToken) => {
        rl.close()

        // Update global variables
        currentAuthToken = authToken.trim()
        authErrorCount = 0 // Reset error count

        console.log('✅ Authentication credentials updated successfully!')
        resolve({ authToken: currentAuthToken })
      }
    )
  })
}

// Function to handle 401 errors and refresh credentials if needed
async function handleAuthError(error, context = '') {
  if (error.response && error.response.status === 401) {
    authErrorCount++
    console.log(
      `🚨 401 Authentication Error #${authErrorCount} ${
        context ? `(${context})` : ''
      }`
    )

    if (authErrorCount >= 10) {
      console.log(
        `\n❌ Received ${authErrorCount} 401 errors. Token has likely expired.`
      )
      await promptForNewCredentials()
      return true // Credentials refreshed
    }
  }
  return false // No refresh needed
}

axios.defaults.timeout = 180000

// Global variables for authentication
let authErrorCount = 0
let currentAuthToken =
  'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJmcCIsImV4cCI6MTc2MjI3OTE2NC4wNTEwMjIzLCJpYXQiOjE3NjIyNzg1NjQuMDUxMDIyMywiZGF0YSI6IntcImNyZWF0ZWRUaW1lXCI6IDE3NTA2NjQ3ODAuMDE5MDI5LCBcInByb2ZpbGVJZFwiOiBcImE4a1dzSkZJNFBrR2NPZzRtcHJIbEwxMEtsWXBuZEo3NjVtbUVZNGxQTnEwZVlQaVdhbkNtNy8yUkgxQmMzcDY1VSszcnMycnhoam00cEQzMENjSUJ3PT0wNmI5MDNiMWU1OTI4NzkxMDRiZmZjNDU0M2U3ZDkzMTdkZTgzNjIyYzhlMTk4ZjljZDAzYzJmMTI1OTJhNDM2XCIsIFwiYW5vbnltb3VzXCI6IHRydWUsIFwidHJhY2VyXCI6IFwiM1E2NVRLUFg1UlwiLCBcInNjb3BlXCI6IFtcIkdVRVNUXCJdLCBcInNpdGVJZFwiOiBcImZwLXVzXCIsIFwiYnJhbmRJZFwiOiBcImZwXCIsIFwic2l0ZUdyb3VwXCI6IFwiZnBcIiwgXCJkYXRhQ2VudGVySWRcIjogXCJVUy1OVlwiLCBcImdlb1JlZ2lvblwiOiBcIkFTLVNHXCIsIFwiZWRnZXNjYXBlXCI6IHtcInJlZ2lvbkNvZGVcIjogXCJQQlwifSwgXCJjYXJ0SWRcIjogXCJwdXVaUlgxUHBLNDhpU2ZsU2dEMFhJSTNCR1FUcEFsRmhlRXhjalVGc2lzcVlGOUkySWtLRXFBNzNzZTRGV0JvNVUrM3JzMnJ4aGptNHBEMzBDY0lCdz09MDU4YjJlYTczM2QxNjk1ZTAzOTI1ZDBmMTljYTRhZmU4NTk5MmZkNGYwNzE1YTdiNDc3NzJjZGE4MTE1ZmQ0M1wifSJ9.cB1J3BdhV64LkL-Uke2HKzsdm1l5PdFFGlQe1ycpxK0'

// Function to fetch FreePeople stores from MongoDB
async function fetchFreePeopleStoresFromServer(page = 1, limit = 1) {
  try {
    console.log(
      `🔍 Fetching FreePeople stores from MongoDB - Page ${page}, Limit ${limit}`
    )

    // Calculate skip value for pagination
    const skip = (page - 1) * limit

    // Fetch stores from MongoDB with pagination and populate products
    const stores = await Store.find({
      storeType: 'freepeople',
    })
      .populate('products')
      .skip(skip)
      .limit(limit)
      .sort({ updatedAt: -1 }) // Sort by most recently updated first
      .lean() // Use lean() for better performance when we don't need mongoose documents

    // Get total count for pagination
    const totalStores = await Store.countDocuments({
      storeType: 'freepeople',
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
        'https://www.freepeople.com/help/returns-exchanges/',
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
const getProductDetails = async (productSlug) => {
  let retryCount = 0
  const maxRetries = 3

  while (retryCount <= maxRetries) {
    try {
      const detailUrl = `https://api.freepeople.com/api/catalog/v1/fp-us/pools/US_DIRECT/products?slug=${productSlug}&projection-slug=pdp&req-info=pdp&countryCode=US`

      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          return await axiosInstance.get(detailUrl, {
            headers: {
              accept: 'application/json, text/plain, */*',
              'accept-language': 'en-US,en;q=0.9',
              authorization: currentAuthToken,
              origin: 'https://www.freepeople.com',
              referer: 'https://www.freepeople.com/',
              'sec-ch-ua':
                '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"',
              'sec-fetch-dest': 'empty',
              'sec-fetch-mode': 'cors',
              'sec-fetch-site': 'same-site',
              'user-agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
              'x-urbn-channel': 'web',
              'x-urbn-country': 'US',
              'x-urbn-currency': 'USD',
              'x-urbn-experience': 'ss',
              'x-urbn-geo-region': 'AS-SG',
              'x-urbn-language': 'en-US',
              'x-urbn-primary-data-center-id': 'US-NV',
              'x-urbn-site-id': 'fp-us',
            },
          })
        },
        5, // maxRetries
        2000, // baseDelay
        'US' // country
      )

      const apiData = response.data
      return apiData[0] // Return first item from array
    } catch (error) {
      const credentialsRefreshed = await handleAuthError(
        error,
        `getProductDetails for ${productSlug}`
      )

      if (credentialsRefreshed && retryCount < maxRetries) {
        retryCount++
        console.log(
          `🔄 Retrying getProductDetails for ${productSlug} with new credentials (attempt ${retryCount}/${maxRetries})`
        )
        continue
      }

      console.error(
        `Error fetching product details for ${productSlug}:`,
        error.message
      )
      return null
    }
  }

  return null
}

// Enhanced generateOutputFiles function with recrawl logic for FreePeople
async function generateOutputFilesWithRecrawl(products, storeData, store) {
  const countryCode = storeData.country || 'US'
  const BATCH_SIZE = 50
  const allProductIds = []
  let allFormattedProducts = []

  products = _.uniqBy(products, (p) => p.product.productId)

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
    `📦 Processing ${products.length} products from FreePeople in batches of ${BATCH_SIZE}...`
  )

  // Track current product IDs from FreePeople
  const currentProductIds = new Set(
    products.map((p) => p.product.productId.toString())
  )
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
    const CONCURRENT_PRODUCT_LIMIT = 10

    // Format products in current batch - process 10 at a time concurrently
    for (let j = 0; j < batch.length; j += CONCURRENT_PRODUCT_LIMIT) {
      const productChunk = batch.slice(j, j + CONCURRENT_PRODUCT_LIMIT)

      console.log(
        `   Processing products ${j + 1}-${Math.min(
          j + CONCURRENT_PRODUCT_LIMIT,
          batch.length
        )} of ${batch.length} in batch ${batchNumber}`
      )

      const productPromises = productChunk.map(async (product) => {
        const productData = product.product
        const productId = productData.productId
        const productUrl = `https://www.freepeople.com/products/${productData.productSlug}`

        // Get existing product from database
        const existingProduct = existingProductsMap.get(productId.toString())

        // Get detailed product information
        const productDetails = await getProductDetails(productData.productSlug)

        // Clean description if available
        let description = ''
        if (productDetails?.product?.longDescription) {
          description = sanitizeHtml(
            productDetails.product.longDescription || '',
            {
              allowedTags: [],
              allowedAttributes: {},
            }
          )
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

        let materials = productDetails?.product?.contents?.join(', ') || ''

        const formattedProduct = {
          parent_product_id: productId.toString(),
          name: productData.displayName,
          description: description,
          category: product._category || '',
          retailer_domain: 'freepeople.com',
          brand: productDetails?.product?.brand || 'Free People',
          gender: product._gender || 'Women',
          materials: materials,
          return_policy_link:
            'https://www.freepeople.com/help/returns-exchanges/',
          return_policy: '',
          size_chart: '',
          available_bank_offers: '',
          available_coupons: '',
          variants: [],
          operation_type: 'NO_CHANGE', // Will be determined based on variant operations
          source: 'freepeople',
          _id:
            existingProduct && existingProduct._id
              ? existingProduct._id
              : new mongoose.Types.ObjectId(),
        }

        // Track variant operations for this product
        const variantOperations = []

        // Process variants (colors and sizes)
        if (productDetails?.skuInfo?.primarySlice?.sliceItems) {
          for (const colorItem of productDetails.skuInfo.primarySlice
            .sliceItems) {
            const colorName = colorItem.displayName || 'Default'
            const originalPrice = productDetails.skuInfo.listPriceHigh || 0
            const sellingPrice = productDetails.skuInfo.salePriceHigh || 0
            const finalPrice = sellingPrice > 0 ? sellingPrice : originalPrice
            const discount = calculateDiscount(originalPrice, sellingPrice)
            const isOnSale = sellingPrice < originalPrice && sellingPrice > 0

            // Get images for this color
            let imageUrl = ''
            let alternateImages = []

            if (colorItem.images && colorItem.images.length > 0) {
              // Get the first image as main image
              const mainImage = colorItem.images[0]
              if (mainImage) {
                imageUrl = `https://images.urbndata.com/is/image/FreePeople/${productDetails.product.styleNumber}_${colorItem.code}_${mainImage}`
              }

              // Get alternate images
              alternateImages = colorItem.images
                .slice(1, 6) // Take up to 5 alternate images
                .map(
                  (img) =>
                    `https://images.urbndata.com/is/image/FreePeople/${productDetails.product.styleNumber}_${colorItem.code}_${img}`
                )
            }

            // Get sizes for this color
            const skus = colorItem.includedSkus || []

            if (skus.length > 0) {
              for (const sku of skus) {
                const isInStock = sku.stockLevel !== 0
                const sizeName = sku.size || ''
                const variantId = `${productId}-${colorItem.code}-${colorName}-${sizeName}`
                currentVariantIds.add(variantId)

                const existingVariant = existingVariantsMap.get(variantId)

                const newVariantData = {
                  price_currency: 'USD',
                  original_price: originalPrice,
                  link_url: productUrl,
                  deeplink_url: productUrl,
                  image_url: imageUrl,
                  alternate_image_urls: alternateImages,
                  is_on_sale: isOnSale,
                  is_in_stock: isInStock,
                  size: sizeName,
                  color: colorName,
                  mpn: uuidv5(
                    `${productId}-${colorName}`,
                    '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
                  ),
                  ratings_count: productDetails.reviews?.count || 0,
                  average_ratings: productDetails.reviews?.averageRating || 0,
                  review_count: productDetails.reviews?.count || 0,
                  selling_price: sellingPrice,
                  sale_price: sellingPrice,
                  final_price: finalPrice,
                  discount: discount,
                  variant_description: '',
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
          }
        }

        // Check for deleted variants (exist in DB but not on FreePeople)
        if (existingProduct) {
          const currentVariantIdsForProduct = new Set()
          if (productDetails?.skuInfo?.primarySlice?.sliceItems) {
            for (const colorItem of productDetails.skuInfo.primarySlice
              .sliceItems) {
              const colorName = colorItem.displayName || 'Default'
              const skus = colorItem.includedSkus || []
              for (const sku of skus) {
                const sizeName = sku.size || ''
                const variantId = `${productId}-${colorItem.code}-${colorName}-${sizeName}`
                currentVariantIdsForProduct.add(variantId)
              }
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
                // Variant exists in DB but not on FreePeople - mark as DELETE
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

        return formattedProduct
      })

      // Wait for all products in this chunk to complete
      const chunkResults = await Promise.all(productPromises)
      formattedProducts.push(...chunkResults)
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
      'https://www.freepeople.com',
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
    'https://www.freepeople.com',
    'US'
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
      storeType: 'freepeople',
      name: 'Free People',
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
      name: storeData.name || 'Free People',
      storeUrl: correctUrl,
      city: '',
      state: '',
      country: storeData.country || 'US',
      products: productIds.map((id) => new mongoose.Types.ObjectId(id)),
      isScrapped: true,
      storeType: 'freepeople',
      returnPolicy: 'https://www.freepeople.com/help/returns-exchanges/',
      tags: ['women', 'fashion', 'clothing'],
    })

    await storeEntry.save()
    console.log(
      `✅ Created new store entry: ${storeData.name || 'Free People'}`
    )
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error updating store entry for ${storeData.name || 'Free People'}:`,
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
  const cleanBrandName = 'freepeople'
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

  // Filter out invalid products
  const filterResults = filterValidProducts(cleanedProducts)
  const finalProducts = filterResults.validProducts

  console.log(
    `📊 Validation: ${filterResults.validCount} valid, ${filterResults.invalidCount} invalid out of ${filterResults.totalCount} total products`
  )

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog-recrawl.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'Free People',
      domain: 'freepeople.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: finalProducts.length,
      original_products_scraped: filterResults.totalCount,
      invalid_products_filtered: filterResults.invalidCount,
      crawled_at: new Date().toISOString(),
      crawl_type: 'RECRAWL',
    },
    products: finalProducts,
  }

  fs.writeFileSync(jsonFilePath, JSON.stringify(catalogData, null, 2), 'utf8')
  console.log(`JSON file generated: ${jsonFilePath}`)

  // Create JSONL file (each product on a separate line)
  const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
  const jsonlContent = finalProducts
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

  return {
    jsonFilePath,
    jsonlFilePath,
    gzippedFilePath,
    filterResults,
  }
}

// Helper function to scrape products from FreePeople categories
async function scrapeFreePeopleCategory(
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
    const offset = (currentPage - 1) * 72
    const pageUrl = categoryConfig.url

    const response = await retryRequestWithProxyRotation(
      async (axiosInstance) => {
        return await axiosInstance.post(
          pageUrl,
          {
            pageSize: 250,
            skip: offset,
            projectionSlug: 'categorytiles',
            personalization: '0',
            customerConsent: 'true',
            featureProductIds: [],
          },
          {
            headers: {
              accept: 'application/json, text/plain, */*',
              'accept-language': 'en-US,en;q=0.9',
              authorization: currentAuthToken,
              'content-type': 'application/json',
              origin: 'https://www.freepeople.com',
              referer: 'https://www.freepeople.com/',
              'sec-ch-ua':
                '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"',
              'sec-fetch-dest': 'empty',
              'sec-fetch-mode': 'cors',
              'sec-fetch-site': 'same-site',
              'user-agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
              'x-urbn-channel': 'web',
              'x-urbn-country': 'US',
              'x-urbn-currency': 'USD',
              'x-urbn-experience': 'ss',
              'x-urbn-geo-region': 'AS-SG',
              'x-urbn-language': 'en-US',
              'x-urbn-pool': 'US_DIRECT',
              'x-urbn-primary-data-center-id': 'US-NV',
              'x-urbn-site-id': 'fp-us',
            },
          }
        )
      },
      5, // maxRetries
      2000, // baseDelay
      'US' // country
    )

    const apiData = response.data
    console.log(`API Response Status: ${response.status}`)

    console.log(
      `Extracting content from ${categoryConfig.name} page ${currentPage}...`
    )

    // Try to parse if it's JSON
    let pageData = null
    let pageProducts = []

    try {
      pageData = apiData

      // Check if we have results
      if (!pageData.records || pageData.records.length === 0) {
        isLastPage = true
        console.log(
          `${categoryConfig.name} Page ${currentPage}: No more results`
        )
        break
      }

      // Extract products from this page
      if (pageData.records && pageData.records.length > 0) {
        pageProducts = pageData.records.map((item) => item.allMeta.tile)

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

// Enhanced main function for recrawling FreePeople
const main = async (store) => {
  try {
    console.log(`🔄 Mode: Recrawl`)

    // Define categories to scrape
    const categories = [
      {
        name: "Women's Dresses",
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/womens-clothes',
      },
      {
        name: 'Jeans',
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/jeans',
      },
      {
        name: "Women's Activewear",
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/all-activewear',
      },
      {
        name: "Women's Swimwear",
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/all-swimwear',
      },
      {
        name: 'Lingerie',
        gender: 'Women',
        url: 'https://api.freepeople.com/api/catalog-search-service/v0/fp-us/tiles/intimates',
      },
    ]

    const storeData = {
      name: 'Free People',
      domain: 'freepeople.com',
      currency: 'USD',
      country: 'US',
    }

    console.log(`Starting to recrawl FreePeople...`)

    const targetProductsPerCategory = 500
    let allProducts = []

    // Scrape each category
    for (const category of categories) {
      console.log(`\n${'='.repeat(50)}`)
      console.log(`🎯 Starting ${category.name} category scraping`)
      console.log(`${'='.repeat(50)}`)

      const categoryProducts = await scrapeFreePeopleCategory(
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
    }

    if (allProducts.length === 0) {
      console.log('⚠️ No products found from any category')
      return false
    }

    console.log(`Total products found: ${allProducts.length}`)

    // Process with recrawl logic
    const result = await generateOutputFilesWithRecrawl(
      allProducts,
      storeData,
      store
    )

    console.log(`Successfully processed FreePeople`)
    console.log(`JSON saved at: ${result.jsonFilePath}`)

    if (result.filterResults) {
      console.log(
        `✅ Final: ${result.filterResults.validCount} valid products saved (${result.filterResults.invalidCount} filtered out)`
      )
    }

    return { result, storeData }
  } catch (e) {
    console.error('Error in recrawling FreePeople store:', e)
    return false
  }
}

// Process FreePeople stores fetched from server
async function processStoresFromServer() {
  console.log(`🚀 Starting to process FreePeople stores from server...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: 0,
    totalPages: 0,
  }

  // For now, create a default FreePeople store entry if none exists
  try {
    const existingStore = await Store.findOne({
      storeType: 'freepeople',
      name: 'Free People',
    })

    console.log('Processing FreePeople store...')
    const storeResult = await main(existingStore)

    if (storeResult && !storeResult.skipped) {
      results.successful.push({
        brandName: storeResult.storeData.name,
        url: 'https://www.freepeople.com',
        region: storeResult.storeData.country,
        jsonPath: storeResult.result.jsonPath,
      })
      console.log('✅ Successfully processed FreePeople')
    } else {
      results.failed.push({
        brandName: 'Free People',
        url: 'https://www.freepeople.com',
        error: 'Processing failed',
      })
      console.log('❌ Failed to process FreePeople')
    }
  } catch (error) {
    results.failed.push({
      brandName: 'Free People',
      url: 'https://www.freepeople.com',
      error: error.message,
    })
    console.log(`❌ Error processing FreePeople: ${error.message}`)
  }

  results.total = 1

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('FREEPEOPLE RECRAWL PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores processed: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Failed: ${results.failed.length}`)

  // Save results to JSON file
  const resultsPath = path.join(
    __dirname,
    'freepeople-recrawl-processing-results.json'
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
  fetchFreePeopleStoresFromServer,
  generateOutputFilesWithRecrawl,
  compareVariants,
  compareProducts,
  getProductDetails,
}

// If run directly from command line
if (require.main === module) {
  console.log('🔄 Processing FreePeople stores from server for RECRAWL...')
  processStoresFromServer()
    .then((results) => {
      console.log('\n🎉 FreePeople store processed!')
      if (results.failed.length > 0) {
        console.log(
          `⚠️  Failed to process store. Check freepeople-recrawl-processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log('🎉 FreePeople store processed successfully!')
      }
    })
    .catch((error) => {
      console.error('Error processing FreePeople stores:', error)
      process.exit(1)
    })
}
