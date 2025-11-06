// Aritzia Products Recrawler - Women's Categories
// Uses Algolia search API to recrawl products from aritzia.com with UPDATE/INSERT/DELETE operations
require('dotenv').config()
const fs = require('fs')
const sanitizeHtml = require('sanitize-html')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')
const path = require('path')
const mongoose = require('mongoose')
const _ = require('lodash')

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
  retryRequestWithProxyRotation,
} = require('./utils/helper')

// Function to fetch Aritzia stores from MongoDB
async function fetchAritziaStoresFromServer(page = 1, limit = 1) {
  try {
    console.log(
      `🔍 Fetching Aritzia stores from MongoDB - Page ${page}, Limit ${limit}`
    )

    // Calculate skip value for pagination
    const skip = (page - 1) * limit

    // Fetch stores from MongoDB with pagination and populate products
    const stores = await Store.find({
      storeType: 'aritzia',
    })
      .populate('products')
      .skip(skip)
      .limit(limit)
      .sort({ updatedAt: -1 }) // Sort by most recently updated first
      .lean() // Use lean() for better performance when we don't need mongoose documents

    // Get total count for pagination
    const totalStores = await Store.countDocuments({
      storeType: 'aritzia',
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
        store.returnPolicy || 'https://www.aritzia.com/intl/en/returns',
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
      storeType: 'aritzia',
      name: 'Aritzia',
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
      name: storeData.name || 'Aritzia',
      storeUrl: correctUrl,
      city: '',
      state: '',
      country: storeData.country || 'US',
      products: productIds.map((id) => new mongoose.Types.ObjectId(id)),
      isScrapped: true,
      storeType: 'aritzia',
      storeTemplate: 'aritzia-template',
      returnPolicy: 'https://www.aritzia.com/intl/en/returns',
      tags: ['women', 'fashion', 'clothing', 'accessories', 'luxury'],
    })

    await storeEntry.save()
    console.log(`✅ Created new store entry: ${storeData.name || 'Aritzia'}`)
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error updating store entry for ${storeData.name || 'Aritzia'}:`,
      error.message
    )
    throw error
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

// Enhanced generateCSV function with recrawl logic for Aritzia
async function generateCSVWithRecrawl(products, storeData, store) {
  const countryCode = storeData.country || 'US'
  const BATCH_SIZE = 50
  const allProductIds = []
  let allFormattedProducts = []

  products = _.uniqBy(products, (p) => p.masterId)

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
    `📦 Processing ${products.length} products from Aritzia in batches of ${BATCH_SIZE}...`
  )

  // Track current product IDs from Aritzia
  const currentProductIds = new Set(products.map((p) => p.masterId.toString()))

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
      const productId = product.masterId
      const existingProduct = existingProductsMap.get(productId.toString())

      console.log(`Processing product: ${product.name}`)

      const formattedProduct = {
        parent_product_id: productId,
        name: product.name,
        description: '',
        category:
          product.categories && product.categories.length > 0
            ? product.categories[0]
            : product._category || '',
        retailer_domain: 'aritzia.com',
        brand: product.brand || 'Aritzia',
        gender: product._gender || 'Women',
        materials:
          product.fabric && product.fabric.length > 0 ? product.fabric[0] : '',
        return_policy_link: 'https://www.aritzia.com/intl/en/returns',
        return_policy: '',
        size_chart: '',
        available_bank_offers: '',
        available_coupons: '',
        variants: [],
        operation_type: 'NO_CHANGE', // Will be determined based on variant operations
        source: 'aritzia',
        _id:
          existingProduct && existingProduct._id
            ? existingProduct._id
            : new mongoose.Types.ObjectId(),
      }

      // Track variant operations for this product
      const variantOperations = []
      const currentVariantIdsForProduct = new Set()

      const variantsToProcess = product.selectableColors

      if (variantsToProcess && variantsToProcess.length > 0) {
        for (const variant of variantsToProcess) {
          // Get color information
          let colorName = variant.value

          // Get pricing information
          const originalPrice = parseFloat(product.price.max || 0)
          const salePrice = parseFloat(variant.prices[0].prices[0] || 0)
          const finalPrice =
            salePrice > 0 && salePrice < originalPrice
              ? salePrice
              : originalPrice
          const isOnSale = salePrice > 0 && salePrice < originalPrice

          // Calculate discount percentage
          let discount = calculateDiscount(originalPrice, salePrice)

          const colorid = Object.keys(variant.colorIds)[0]
          const variantImages = variant.colorIds[colorid]

          let imageUrl = ''
          if (variantImages && variantImages.length > 0) {
            imageUrl = `https://assets.aritzia.com/image/upload/c_crop,ar_1920:2623,g_south/q_auto,f_auto,dpr_auto,w_900/${variantImages[0]}`
          }

          let alternateImages = [imageUrl]
          if (variantImages && Array.isArray(variantImages)) {
            alternateImages = variantImages.map(
              (img) =>
                `https://assets.aritzia.com/image/upload/c_crop,ar_1920:2623,g_south/q_auto,f_auto,dpr_auto,w_900/${img}`
            )
          }

          const sizes = variant.sizeRun

          for (const size of sizes) {
            const variantId = `${productId}-${colorid}-${size}`
            currentVariantIdsForProduct.add(variantId)

            const existingVariant = existingVariantsMap.get(variantId)

            // Build product URL
            let variantUrl = `https://www.aritzia.com/intl/en/product/${product.slug}?color=${colorid}`

            const newVariantData = {
              variant_description: '',
              price_currency: 'USD',
              original_price: originalPrice,
              link_url: variantUrl,
              deeplink_url: variantUrl,
              image_url: imageUrl,
              alternate_image_urls: alternateImages,
              is_on_sale: isOnSale,
              is_in_stock: true,
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

      // Check for deleted variants (exist in DB but not on Aritzia)
      if (existingProduct) {
        if (
          existingProduct.variants &&
          Array.isArray(existingProduct.variants)
        ) {
          existingProduct.variants.forEach((existingVariant) => {
            if (
              existingVariant &&
              !currentVariantIdsForProduct.has(existingVariant.variant_id)
            ) {
              // Variant exists in DB but not on Aritzia - mark as DELETE
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
      'https://www.aritzia.com',
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
    'https://www.aritzia.com',
    countryCode
  )

  console.log(
    `\n📊 Recrawl Results: ${allProductIds.length} products processed`
  )

  return { jsonPath: outputResult.gzippedFilePath, productIds: allProductIds }
}

// Function to generate output files
async function generateOutputFiles(
  allFormattedProducts,
  storeData,
  correctUrl,
  countryCode
) {
  // Create directory structure: countryCode/retailername-countryCode/
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

  // Remove MongoDB _id from output
  const cleanedProducts = allFormattedProducts.map((item) => {
    const { _id, ...itemWithoutId } = item
    return itemWithoutId
  })

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog-recrawl.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'Aritzia',
      domain: 'aritzia.com',
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

// Enhanced main function for recrawling Aritzia
const main = async (store) => {
  try {
    console.log(`🔄 Mode: Recrawl`)

    const storeData = {
      name: 'Aritzia',
      domain: 'aritzia.com',
      currency: 'USD',
      country: 'US',
    }

    console.log(`Starting to recrawl Aritzia...`)

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

    const targetProductsPerCategory = 400

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

    if (allProducts.length === 0) {
      console.log(`No products found`)
      return false
    }

    console.log(`Total products found: ${allProducts.length}`)

    // Process with recrawl logic
    const result = await generateCSVWithRecrawl(allProducts, storeData, store)

    console.log(`Successfully processed Aritzia`)
    console.log(`JSON saved at: ${result.jsonPath}`)

    return { result, storeData }
  } catch (e) {
    console.error('Error in recrawling Aritzia store:', e)
    return false
  }
}

// Process Aritzia stores fetched from server
async function processStoresFromServer() {
  console.log(`🚀 Starting to process Aritzia stores from server...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: 0,
    totalPages: 0,
  }

  // For now, create a default Aritzia store entry if none exists
  try {
    const existingStore = await Store.findOne({
      storeType: 'aritzia',
      name: 'Aritzia',
    })

    console.log('Processing Aritzia store...')
    const storeResult = await main(existingStore)

    if (storeResult && !storeResult.skipped) {
      results.successful.push({
        brandName: storeResult.storeData.name,
        url: 'https://www.aritzia.com',
        region: storeResult.storeData.country,
        jsonPath: storeResult.result.jsonPath,
      })
      console.log('✅ Successfully processed Aritzia')
    } else {
      results.failed.push({
        brandName: 'Aritzia',
        url: 'https://www.aritzia.com',
        error: 'Processing failed',
      })
      console.log('❌ Failed to process Aritzia')
    }
  } catch (error) {
    results.failed.push({
      brandName: 'Aritzia',
      url: 'https://www.aritzia.com',
      error: error.message,
    })
    console.log(`❌ Error processing Aritzia: ${error.message}`)
  }

  results.total = 1

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('ARITZIA RECRAWL PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores processed: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Failed: ${results.failed.length}`)

  // Save results to JSON file
  const resultsPath = path.join(
    __dirname,
    'aritzia-recrawl-processing-results.json'
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
  fetchAritziaStoresFromServer,
  generateCSVWithRecrawl,
  compareVariants,
  compareProducts,
  scrapeAritziaCategory,
}

// If run directly from command line
if (require.main === module) {
  console.log('🔄 Processing Aritzia stores from server for RECRAWL...')
  processStoresFromServer()
    .then((results) => {
      console.log('\n🎉 Aritzia store processed!')
      if (results.failed.length > 0) {
        console.log(
          `⚠️  Failed to process store. Check aritzia-recrawl-processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log('🎉 Aritzia store processed successfully!')
      }
    })
    .catch((error) => {
      console.error('Error processing Aritzia stores:', error)
      process.exit(1)
    })
}
