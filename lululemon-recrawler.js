// Lululemon Products Recrawler - Multiple Categories
// Recrawls products from various Lululemon categories using GraphQL with INSERT/UPDATE/DELETE operations
require('dotenv').config()
const axios = require('axios')
const fs = require('fs')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')
const path = require('path')
const mongoose = require('mongoose')
const _ = require('lodash')
const { filterValidProducts } = require('./validate-catalog')

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

// Configuration object for the category API client
const config = {
  baseURL: 'https://shop.lululemon.com/snb/graphql',
  headers: {
    accept: 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    origin: 'https://shop.lululemon.com',
    priority: 'u=1, i',
    referer:
      'https://shop.lululemon.com/c/women-shorts/n11ybt?icid=lp-story:women;L1;l2;cdp:womens-shorts;',
    'sec-ch-ua':
      '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent':
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-lll-client': 'cdp-api',
  },
}

// Configuration object for the product details API client
const productConfig = {
  baseURL: 'https://shop.lululemon.com/cne/graphql',
  headers: {
    accept: 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    origin: 'https://shop.lululemon.com',
    priority: 'u=1, i',
    referer:
      'https://shop.lululemon.com/c/women-shorts/n11ybt?icid=lp-story:women;L1;l2;cdp:womens-shorts;',
    'sec-ch-ua':
      '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent':
      'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    'x-lll-client': 'product-sdk',
    'x-lll-ecom-correlation-id': 'nonprod',
    'x-lll-request-correlation-id': '816c17c2-6d85-4c31-8705-a7b1c712502a',
  },
}

// GraphQL query for category data
const categoryQuery = `
query CategoryPageDataQuery(
  $category: String!
  $cid: String
  $forceMemberCheck: Boolean
  $nValue: String
  $cdpHash: String
  $sl: String!
  $locale: String!
  $Ns: String
  $storeId: String
  $pageSize: Int
  $page: Int
  $onlyStore: Boolean
  $useHighlights: Boolean
  $abFlags: [String]
  $styleboost: [String]
  $fusionExperimentVariant: String
) {
  categoryPageData(
    category: $category
    nValue: $nValue
    cdpHash: $cdpHash
    locale: $locale
    sl: $sl
    Ns: $Ns
    page: $page
    pageSize: $pageSize
    storeId: $storeId
    onlyStore: $onlyStore
    forceMemberCheck: $forceMemberCheck
    cid: $cid
    useHighlights: $useHighlights
    abFlags: $abFlags
    styleboost: $styleboost
    fusionExperimentVariant: $fusionExperimentVariant
  ) {
    activeCategory
    allLocaleNvalues {
      CA
      US
    }
    categoryLabel
    fusionExperimentId
    fusionExperimentVariant
    fusionQueryId
    h1Title
    isBopisEnabled
    isFusionQuery
    isWMTM
    name
    results: totalProducts
    totalProductPages
    currentPage
    type
    products {
      allAvailableSizes
      currencyCode
      defaultSku
      displayName
      intendedCupSize
      listPrice
      parentCategoryUnifiedId
      productOnSale: onSale
      productSalePrice: salePrice
      pdpUrl
      productCoverage
      repositoryId: productId
      productId
      inStore
      unifiedId
      highlights {
        highlightLabel
        highlightIconWeb
        priority
        visibility
        subText
        abFlag {
          abFlagName
          showIcon
          showHighlight
          showSubText
          visibility
        }
      }
      skuStyleOrder {
        colorGroup
        colorId
        colorName
        inStore
        size
        sku
        skuStyleOrderId
        styleId01
        styleId02
        styleId
      }
      swatches {
        primaryImage
        hoverImage
        url
        colorId
        inStore
      }
    }
  }
}`

// GraphQL query for product details
const productQuery = `
query GetPdpDataById(
  $id: String!
  $category: String!
  $unifiedId: String!
  $locale: String
  $forceMemberCheck: Boolean
  $sl: String
  $forcePcm: Boolean
  $fetchPcmMedia: Boolean!
  $fetchVariants: Boolean!
) {
  productDetailPage(
    id: $id
    category: $category
    unifiedId: $unifiedId
    locale: $locale
    forceMemberCheck: $forceMemberCheck
    sl: $sl
    forcePcm: $forcePcm
  ) {
    category {
      id
      name
    }
    colors {
      code
      name
      swatchUrl
      slug
      simpleRanking
      ituBt709
      ituBt601
      colorHarmonyRank
    }
   colorDriver {
      color
      sizes
    }
    colorAttributes {
      colorId
      styleColorId
      wwmt
      fabricPill
      colorGroups
      designedFor {
        activityText
        iconId
      }
      careAndContent {
        iconId
        title
        sections {
          media
          title
          attributes {
            badgeId
            badgeText
            iconId
            list {
              items
              title
            }
            text
          }
        }
      }
      fabricOrBenefits {
        iconId
        title
        sections {
          media {
            captionText
            imageAlt
            videoSrcPortrait
            videoPosterSrc
            videoPosterSrcPortrait
            imageSrc
            videoSrc
          }
          title
          attributes {
            iconId
            list
            text
            attributeType
          }
        }
      }
      fitOrIngredients {
        id
        iconId
        title
        sections {
          media {
            captionText
            imageAlt
            videoSrcPortrait
            videoPosterSrc
            videoPosterSrcPortrait
            imageSrc
            videoSrc
          }
          title
          attributes {
            iconId
            list
            text
            attributeType
          }
        }
      }
      fitOrHowToUse {
        id
        iconId
        title
        sections {
          media {
            captionText
            imageAlt
            videoSrcPortrait
            videoPosterSrc
            videoPosterSrcPortrait
            imageSrc
            videoSrc
          }
          title
          attributes {
            iconId
            list
            text
            attributeType
          }
        }
      }
      featuresOrIngredients {
        id
        iconId
        title
        sections {
          media {
            captionText
            imageAlt
            videoSrcPortrait
            videoPosterSrc
            videoPosterSrcPortrait
            imageSrc
            videoSrc
          }
          title
          attributes {
            iconId
            list
            text
            attributeType
          }
        }
      }
    }
    highlights {
      highlightIconWeb
      highlightLabel
      visibility
      subText
      abFlag {
        name
        abFlagName
        showIcon
        showSubText
        showHighlight
        visibility
      }
    }
    productAttributes {
      productContentCare {
        colorId
        heroBannerHotSpotText
        care {
          careId
          careDescription
          iconImgUrl
        }
      }
      productContentFeature {
        f5ContentAlignmentPDP
        styleId
        f5Features {
          featureName
          featureDescription
        }
      }
      productContentFabric {
        fabricDescription
        fabricDisplayName
        fabricId
        fabricPurposes
      }
      productContentWhyWeMadeThis
    }
    productCarousel {
      color {
        code
        name
        swatchUrl
        slug
        simpleRanking
        ituBt709
        ituBt601
        colorHarmonyRank
      }
      modelInfo {
        description
        modelIsWearing {
          productName
          numberOfAvailableColors
          url
          imageURL
          onSale
          listPrice
          salePrice
        }
        shopThisLook
      }
      imageInfo
      inseam
      mediaInfo {
        type
        url
        posterImageUrl
      }
      pcmMediaInfo @include(if: $fetchPcmMedia) {
        type
        url
        posterImageUrl
      }
    }
    productSummary {
      productId
      displayName
      unifiedId
      parentCategoryUnifiedId
      pdpUrl
      pdpUrlWithRegion
      productUrl
      shareUrl
      whyWeMadeThis
      isFinalSale
      isSoldOut
      isLoyaltyProduct
      isHazmatProduct
      divisionId
      featuredFabric
      fitDetails
      departmentId
      activity
      allAncestorsDisplayName
      allAncestorsRepositoryId
      allAvailableSizes
      bazaarVoiceID
      collections
      colorGroup
      colour
      commonId
      currencyCode
      defaultParentCategory
      defaultSku
      display
      displayCA
      displayNameWithBr
      f5BckimgUrl
      freeReturnShipping
      gender
      genderCategoryTitle
      genderCategoryProductTitle
      hasLinkedProducts
      imageScheme
      isDisplayble
      isProductLocaleMatch
      itemType
      linkedProducts
      listPrice
      locale
      loyaltyTermsAndConditions {
        url
        text
      }
      onSale
      parentCategoryDisplayName
      parentCategoryKeywords
      price
      priceRange
      productActivityIdRepositoryId
      productApplicableSystems
      productBaseUrl
      productCatalogId
      productCategory
      productDefaultSort
      productDisallowAsRecommendation
      productHasOutfitProduct
      productLanguage
      productLastSkuAdditionDateTime
      productMarkDown
      productName
      productNoFollow
      productNoIndex
      productNonReturnable
      productNumberOfImageAssets
      productOnSale
      productSilver
      productSiteMapPdpUrl
      productSizes
      productWhatsNew
      skuOnSale
      skuSkuImages
      skuStyleOrder
      title
      trendingColorsAll
      type
    }
    skus {
      id
      skuUrl
      price {
        listPrice
        currency {
          code
          symbol
        }
        onSale
        salePrice
        earlyAccessMarkdownPrice
      }
      size
      color {
        code
        name
        swatchUrl
        slug
        simpleRanking
        ituBt709
        ituBt601
        colorHarmonyRank
      }
      available
      inseam
      styleId
      styleNumber
    }
    variants @include(if: $fetchVariants) {
      color {
        code
        name
        swatchUrl
        slug
        simpleRanking
        ituBt709
        ituBt601
        colorHarmonyRank
      }
      skus {
        id
        skuUrl
        price {
          listPrice
          currency {
            code
            symbol
          }
          onSale
          salePrice
          earlyAccessMarkdownPrice
        }
        size
        color {
          code
          name
          swatchUrl
          slug
          simpleRanking
          ituBt709
          ituBt601
          colorHarmonyRank
        }
        available
        inseam
        styleId
        styleNumber
      }
      imageSet {
        images {
          url
          alt
        }
        modelDescription
      }
    }
  }
}`

// Default variables for category API
const defaultVariables = {
  pageSize: 48, // Increased page size for efficiency
  page: 1,
  useHighlights: true,
  onlyStore: false,
  abFlags: ['cdpSeodsEnabled'],
  category: 'women-shorts',
  cdpHash: 'n11ybt',
  forceMemberCheck: false,
  fusionExperimentVariant: '',
  locale: 'en_US',
  Ns: '',
  nValue: null,
  sl: 'US',
  storeId: null,
  styleboost: [],
}

// Default variables for product details API
const defaultProductVariables = {
  id: '',
  category: '',
  unifiedId: '',
  locale: 'en-us',
  forceMemberCheck: false,
  sl: null,
  forcePcm: true,
  fetchPcmMedia: false,
  fetchVariants: true, // Enable variants to get all color/size combinations
}

// Helper function to chunk array into smaller arrays
const chunkArray = (array, chunkSize) => {
  const chunks = []
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize))
  }
  return chunks
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

// Main function to call the category API
async function fetchCategoryData(variables = {}) {
  try {
    const mergedVariables = { ...defaultVariables, ...variables }

    const response = await axios.post(
      config.baseURL,
      {
        query: categoryQuery,
        variables: mergedVariables,
      },
      {
        headers: config.headers,
      }
    )

    return response.data
  } catch (error) {
    console.error(
      'Error fetching category data:',
      error.response?.data || error.message
    )
    throw error
  }
}

// Main function to call the product details API
async function fetchProductDetails(variables = {}) {
  try {
    const mergedVariables = { ...defaultProductVariables, ...variables }

    const response = await axios.post(
      productConfig.baseURL,
      {
        query: productQuery,
        variables: mergedVariables,
      },
      {
        headers: productConfig.headers,
      }
    )

    return response.data
  } catch (error) {
    console.error(
      'Error fetching product details:',
      error.response?.data || error.message
    )
    throw error
  }
}

// Helper function to get detailed product information
const getProductDetails = async (product) => {
  try {
    if (!product.productId || !product.unifiedId) {
      console.log(
        `Missing product data for: ${product.displayName || 'Unknown'}`
      )
      return null
    }

    console.log(`Fetching detailed info for: ${product.displayName}`)

    // Get product details using GraphQL
    const detailData = await fetchProductDetails({
      id: product.productId,
    })

    return detailData
  } catch (error) {
    console.error(
      `Error fetching product details for ${product.displayName}:`,
      error.message
    )
    return null
  }
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
      storeType: 'lululemon',
      name: 'Lululemon',
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
      name: storeData.name || 'Lululemon',
      storeUrl: correctUrl,
      city: '',
      state: '',
      country: storeData.country || 'US',
      products: productIds.map((id) => new mongoose.Types.ObjectId(id)),
      isScrapped: true,
      storeType: 'lululemon',
      storeTemplate: 'lululemon-template',
      returnPolicy: 'https://shop.lululemon.com/help/returns-and-refunds',
      tags: ['women', 'men', 'athleisure', 'yoga', 'fitness'],
    })

    await storeEntry.save()
    console.log(`✅ Created new store entry: ${storeData.name || 'Lululemon'}`)
    return storeEntry
  } catch (error) {
    console.error(
      `❌ Error updating store entry for ${storeData.name || 'Lululemon'}:`,
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
    const cleanBrandName = 'lululemon'
    const dirPath = path.join(
      __dirname,
      'output',
      countryCode,
      `${cleanBrandName}-${countryCode}`
    )

    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true })
    }

    // Create JSONL file (each product on a separate line) - streaming approach
    const jsonFilePath = path.join(dirPath, 'catalog.json')
    const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')

    // Use streaming to avoid memory issues
    const writeStream = fs.createWriteStream(jsonlFilePath, {
      encoding: 'utf8',
    })

    for (let i = 0; i < allFormattedProducts.length; i++) {
      const product = allFormattedProducts[i]
      // Remove MongoDB _id from each product individually
      const { _id, ...cleanProduct } = product

      // Write each product as a line, add newline except for last item
      const jsonLine = JSON.stringify(cleanProduct)
      if (i === allFormattedProducts.length - 1) {
        writeStream.write(jsonLine)
      } else {
        writeStream.write(jsonLine + '\n')
      }
    }

    // Close the write stream and wait for it to finish
    await new Promise((resolve, reject) => {
      writeStream.end((error) => {
        if (error) reject(error)
        else resolve()
      })
    })

    console.log(`JSONL file generated: ${jsonlFilePath}`)

    // Gzip the JSONL file using streams to avoid loading entire file into memory
    const gzippedFilePath = `${jsonlFilePath}.gz`

    await new Promise((resolve, reject) => {
      const readStream = fs.createReadStream(jsonlFilePath)
      const gzipStream = zlib.createGzip()
      const writeGzipStream = fs.createWriteStream(gzippedFilePath)

      readStream
        .pipe(gzipStream)
        .pipe(writeGzipStream)
        .on('finish', resolve)
        .on('error', reject)
    })

    console.log(`Gzipped JSONL file generated: ${gzippedFilePath}`)

    return { jsonFilePath, jsonlFilePath: jsonlFilePath, gzippedFilePath }
  } catch (error) {
    console.error('❌ Error in generateOutputFiles:', error.message)
    throw error
  }
}

// Helper function to scrape products from a specific category
async function scrapeLululemonCategory(
  categoryConfig,
  targetProductCount = 1500
) {
  let currentPage = 1
  let allProducts = []
  let isLastPage = false

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (!isLastPage && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} page ${currentPage}...`)

    try {
      // Fetch category data using GraphQL
      const pageData = await fetchCategoryData({
        category: categoryConfig.category,
        cdpHash: categoryConfig.cdpHash,
        page: currentPage,
        pageSize: 40, // Use larger page size for efficiency
      })

      if (pageData?.data?.categoryPageData) {
        const categoryData = pageData.data.categoryPageData

        // Check pagination info
        isLastPage = currentPage >= (categoryData.totalProductPages || 1)
        console.log(
          `${categoryConfig.name} Page ${currentPage}: isLastPage = ${isLastPage}`
        )
        console.log(
          `Total pages: ${categoryData.totalProductPages || 'unknown'}`
        )

        // Extract products from this page
        if (categoryData.products && categoryData.products.length > 0) {
          const pageProducts = categoryData.products

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
          `Successfully fetched data for ${categoryConfig.name} page ${currentPage}`
        )
      } else {
        console.log(
          `No category data found for ${categoryConfig.name} page ${currentPage}`
        )
        isLastPage = true
      }
    } catch (error) {
      console.log(
        `Error fetching data for ${categoryConfig.name} page ${currentPage}:`,
        error.message
      )
      isLastPage = true
    }

    currentPage++
    // Add a small delay between requests to be respectful
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

// Generate files with recrawl logic for combined products from all categories
async function generateCombinedFilesWithRecrawl(products, storeData, store) {
  const countryCode = storeData.country || 'US'
  const BATCH_SIZE = 20
  const allProductIds = []
  let allFormattedProducts = []

  // Remove duplicates based on productId
  products = _.uniqBy(products, (p) => p.productId)

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
    `📦 Processing ${products.length} products from Lululemon in batches of ${BATCH_SIZE}...`
  )

  // Track current product IDs from Lululemon
  const currentProductIds = new Set(products.map((p) => p.productId.toString()))
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
      const productId = product.productId?.toString()
      const productUrl = `https://shop.lululemon.com${product.pdpUrl}`
      const gender = product._gender || 'Women'
      const category = product._category || 'Unknown'

      // Get existing product from database
      const existingProduct = existingProductsMap.get(productId)

      // Get detailed product information
      const productDetails = await getProductDetails(product)

      // Extract description and materials
      let description = ''
      let materials = ''

      if (
        productDetails?.data?.productDetailPage?.productSummary?.whyWeMadeThis
      ) {
        description =
          productDetails.data.productDetailPage.productSummary.whyWeMadeThis
      }

      if (
        productDetails?.data?.productDetailPage?.colorAttributes &&
        productDetails?.data?.productDetailPage?.colorAttributes.length > 0
      ) {
        if (
          productDetails?.data?.productDetailPage?.colorAttributes[0]
            .careAndContent
        ) {
          const careContent =
            productDetails?.data?.productDetailPage?.colorAttributes[0]
              .careAndContent

          const materialsAttribute = careContent.sections.find(
            (item) => item.title == 'Materials'
          )

          if (materialsAttribute) {
            materialsAttribute.attributes.map((item) => {
              const materialType = item.list.items.join(' ')
              const partType = item.list?.title?.toLowerCase()
              materials += `${partType}: ${materialType} , `
            })
          }
        }
      }

      const formattedProduct = {
        parent_product_id: productId,
        name: product.displayName,
        description: description,
        category: category,
        retailer_domain: 'shop.lululemon.com',
        brand: 'Lululemon',
        gender: gender,
        materials: materials,
        return_policy_link:
          'https://shop.lululemon.com/help/returns-and-refunds',
        return_policy: '',
        size_chart: '',
        available_bank_offers: '',
        available_coupons: '',
        variants: [],
        operation_type: 'NO_CHANGE', // Will be determined based on variant operations
        source: 'lululemon',
        _id:
          existingProduct && existingProduct._id
            ? existingProduct._id
            : new mongoose.Types.ObjectId(),
      }

      // Track variant operations for this product
      const variantOperations = []

      // Process variants (colors and sizes)
      if (
        productDetails?.data?.productDetailPage?.colors &&
        productDetails.data.productDetailPage.colors.length > 0
      ) {
        for (const variant of productDetails.data.productDetailPage.colors) {
          const colorName = variant.name
          const originalPrice =
            product.listPrice && product.listPrice.length > 0
              ? parseFloat(product.listPrice[0]).toFixed(2)
              : 0
          const sellingPrice =
            product.productSalePrice && product.productSalePrice.length > 0
              ? parseFloat(product.productSalePrice[0]).toFixed(2)
              : 0

          const finalPrice =
            sellingPrice > 0 && sellingPrice < originalPrice
              ? sellingPrice
              : originalPrice
          const discount = calculateDiscount(originalPrice, sellingPrice)

          const isOnSale = product.productOnSale || false

          // Get images for this variant
          let imageUrl = ''
          let alternateImages = []

          if (productDetails?.data?.productDetailPage?.productCarousel) {
            const colorImages =
              productDetails?.data?.productDetailPage?.productCarousel.find(
                (item) => item.color.code == variant.code
              )

            if (colorImages) {
              imageUrl = colorImages.imageInfo[0]
              alternateImages = colorImages.imageInfo.slice(1, 6)
            }
          }

          let colorSizes =
            productDetails?.data?.productDetailPage?.colorDriver.find(
              (item) => item.color == variant.code
            )

          if (colorSizes) {
            colorSizes = colorSizes.sizes
              .filter((size) => {
                if (typeof parseInt(size) == 'number' && size > 12) return false
                else return true
              })
              .map((size) => {
                if (size == 0) {
                  return { name: 'XXXS' }
                } else if (size == 2) {
                  return { name: 'XXS' }
                } else if (size == 4) {
                  return { name: 'XS' }
                } else if (size == 6) {
                  return { name: 'S' }
                } else if (size == 8) {
                  return { name: 'M' }
                } else if (size == 10) {
                  return { name: 'L' }
                } else if (size == 12) {
                  return { name: 'XL' }
                } else {
                  return { name: size }
                }
              })
          }

          // Process SKUs for this variant
          if (colorSizes && colorSizes.length > 0) {
            for (const variantSize of colorSizes) {
              const isInStock = true
              const size = variantSize.name || ''
              const variantId = `${productId}-${colorName}-${size}`
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
                size: size,
                color: colorName,
                mpn: uuidv5(
                  `${productId}-${colorName}`,
                  '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
                ),
                ratings_count: 0,
                average_ratings: 0,
                review_count: 0,
                selling_price: finalPrice,
                sale_price: sellingPrice,
                final_price: finalPrice,
                discount: discount,
                variant_id: variantId,
                variant_description: '',
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

      // Check for deleted variants (exist in DB but not on Lululemon)
      if (existingProduct) {
        if (
          existingProduct.variants &&
          Array.isArray(existingProduct.variants)
        ) {
          existingProduct.variants.forEach((existingVariant) => {
            if (
              existingVariant &&
              !currentVariantIds.has(existingVariant.variant_id)
            ) {
              // Variant exists in DB but not on Lululemon - mark as DELETE
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
      console.log('Waiting 2 seconds before processing next batch...')
      await new Promise((resolve) => setTimeout(resolve, 2000))
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
      'https://shop.lululemon.com',
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
    'https://shop.lululemon.com',
    countryCode
  )

  console.log(
    `\n📊 Recrawl Results: ${allProductIds.length} products processed, ${filterResult.validCount} valid products saved`
  )

  return { jsonPath: outputResult.gzippedFilePath, productIds: allProductIds }
}

async function recrawlLululemonProducts(store) {
  try {
    console.log('🚀 Starting Lululemon recrawling...')

    // Define categories to scrape
    const categories = [
      {
        name: 'Women Shorts',
        gender: 'Women',
        category: 'women-shorts',
        cdpHash: 'n11ybt',
      },
      {
        name: "All Women's Clothes",
        gender: 'Women',
        category: 'all-women-clothes',
        cdpHash: 'n14uwk',
      },
      {
        name: 'Workout Clothes',
        gender: 'Women',
        category: 'women-workout-clothes',
        cdpHash: 'n14uwkzae4c',
      },
      {
        name: 'Women Dresses',
        gender: 'Women',
        category: 'women-dresses',
        cdpHash: 'n1mk31',
      },
      {
        name: 'Women Whats New',
        gender: 'Women',
        category: 'women-whats-new',
        cdpHash: 'n16o10zq0cf',
      },
      {
        name: 'Women We Made Too Much',
        gender: 'Women',
        category: 'women-we-made-too-much',
        cdpHash: 'n16o10z8mhd',
      },
      {
        name: 'Mens Casual Clothes',
        gender: 'Men',
        category: 'men-casual-clothes',
        cdpHash: 'n1oxc7zyk1r',
      },
      {
        name: 'Men We Made Too Much',
        gender: 'Men',
        category: 'men-we-made-too-much',
        cdpHash: 'n18mhdznrqw',
      },
    ]

    const targetProductsPerCategory = 800

    const storeData = {
      name: 'Lululemon',
      domain: 'shop.lululemon.com',
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

      const categoryProducts = await scrapeLululemonCategory(
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
    console.log('🎯 PROCESSING ALL PRODUCTS TOGETHER WITH RECRAWL LOGIC 🎯')
    console.log(`${'🎯'.repeat(20)}`)
    console.log(`📦 Total products collected: ${allProducts.length}`)
    allProductDetails.forEach((detail) => {
      console.log(`   ${detail.category}: ${detail.count} products`)
    })
    console.log(
      `📦 Note: Duplicate products will be removed based on product IDs before processing`
    )

    if (allProducts.length === 0) {
      console.log('⚠️ No products found from any category')
      return false
    }

    // Process all products together with recrawl logic
    const combinedFilesResult = await generateCombinedFilesWithRecrawl(
      allProducts,
      storeData,
      store
    )

    console.log(`\n${'🎉'.repeat(20)}`)
    console.log('🎉 ALL LULULEMON RECRAWLING COMPLETED SUCCESSFULLY! 🎉')
    console.log(`${'🎉'.repeat(20)}`)

    console.log(`\n📊 Recrawl Results Summary:`)
    console.log(`   Total Products Processed: ${allProducts.length}`)
    console.log(`   Output Files: ${combinedFilesResult.jsonPath}`)

    return {
      categories: allProductDetails,
      totalProducts: allProducts.length,
      jsonPath: combinedFilesResult.jsonPath,
      productIds: combinedFilesResult.productIds,
    }
  } catch (error) {
    console.error('❌ Error during recrawling:', error)
    throw error
  }
}

// Process Lululemon stores fetched from server
async function processStoresFromServer() {
  console.log(`🚀 Starting to process Lululemon stores from server...`)
  await connectDB()

  const results = {
    successful: [],
    failed: [],
    skipped: [],
    total: 0,
    totalPages: 0,
  }

  // For now, create a default Lululemon store entry if none exists
  try {
    const existingStore = await Store.findOne({
      storeType: 'lululemon',
      name: 'Lululemon',
    }).populate('products')

    console.log('Processing Lululemon store...')
    const storeResult = await recrawlLululemonProducts(existingStore)

    if (storeResult && !storeResult.skipped) {
      results.successful.push({
        brandName: 'Lululemon',
        url: 'https://shop.lululemon.com',
        region: 'US',
        jsonPath: storeResult.jsonPath,
      })
      console.log('✅ Successfully processed Lululemon')
    } else {
      results.failed.push({
        brandName: 'Lululemon',
        url: 'https://shop.lululemon.com',
        error: 'Processing failed',
      })
      console.log('❌ Failed to process Lululemon')
    }
  } catch (error) {
    results.failed.push({
      brandName: 'Lululemon',
      url: 'https://shop.lululemon.com',
      error: error.message,
    })
    console.log(`❌ Error processing Lululemon: ${error.message}`)
  }

  results.total = 1

  // Generate summary report
  console.log('\n' + '='.repeat(80))
  console.log('LULULEMON RECRAWL PROCESSING SUMMARY')
  console.log('='.repeat(80))
  console.log(`Total stores processed: ${results.total}`)
  console.log(`Successful: ${results.successful.length}`)
  console.log(`Failed: ${results.failed.length}`)

  // Save results to JSON file
  const resultsPath = path.join(
    __dirname,
    'lululemon-recrawl-processing-results.json'
  )
  fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2))
  console.log(`\nDetailed results saved to: ${resultsPath}`)

  await disconnectDB()
  return results
}

// Export functions
module.exports = {
  processStoresFromServer,
  recrawlLululemonProducts,
  compareVariants,
  compareProducts,
  determineProductOperationType,
}

// If run directly from command line
if (require.main === module) {
  console.log('🔄 Processing Lululemon stores from server for RECRAWL...')
  processStoresFromServer()
    .then((results) => {
      console.log('\n🎉 Lululemon store processed!')
      if (results.failed.length > 0) {
        console.log(
          `⚠️  Failed to process store. Check lululemon-recrawl-processing-results.json for details.`
        )
        process.exit(1)
      } else {
        console.log('🎉 Lululemon store processed successfully!')
      }
    })
    .catch((error) => {
      console.error('Error processing Lululemon stores:', error)
      process.exit(1)
    })
}
