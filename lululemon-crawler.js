// Lululemon Products Scraper - Multiple Categories
// Scrapes products from various Lululemon categories using GraphQL
require('dotenv').config()
const axios = require('axios')
const fs = require('fs')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const zlib = require('zlib')
const path = require('path')

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

// Helper function to process a single product
const processProduct = async (product, gender = 'Women', category = '') => {
  console.log(`Processing product: ${product.displayName}`)

  const productId = product.productId?.toString()
  const productUrl = `https://shop.lululemon.com${product.pdpUrl}`

  // Get detailed product information
  const productDetails = await getProductDetails(product)

  // Extract description and materials
  let description = ''
  let materials = ''

  if (productDetails?.data?.productDetailPage?.productSummary?.whyWeMadeThis) {
    description =
      productDetails.data.productDetailPage.productSummary.whyWeMadeThis
  }

  if (
    productDetails?.data?.productDetailPage?.colorAttributes &&
    productDetails?.data?.productDetailPage?.colorAttributes.length > 0
  ) {
    if (
      productDetails?.data?.productDetailPage?.colorAttributes[0].careAndContent
    ) {
      const careContent =
        productDetails?.data?.productDetailPage?.colorAttributes[0]
          .careAndContent

      const materialsAttribute = careContent.sections.find(
        (item) => item.title == 'Materials'
      )

      materialsAttribute.attributes.map((item) => {
        const materialType = item.list.items.join(' ')
        const partType = item.list.title.toLowerCase()
        materials += `${partType}: ${materialType} , `
      })
    }
  }

  // Determine category from product data
  const formattedProduct = {
    parent_product_id: productId,
    name: product.displayName,
    description: description,
    category: category,
    retailer_domain: 'shop.lululemon.com',
    brand: 'Lululemon',
    gender: gender,
    materials: materials,
    return_policy_link: 'https://shop.lululemon.com/help/returns-and-refunds',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'lululemon',
  }

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
        // Get the first image as main image
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
              return {
                name: 'XXXS',
              }
            } else if (size == 2) {
              return {
                name: 'XXS',
              }
            } else if (size == 4) {
              return {
                name: 'XS',
              }
            } else if (size == 6) {
              return {
                name: 'S',
              }
            } else if (size == 8) {
              return {
                name: 'M',
              }
            } else if (size == 10) {
              return {
                name: 'L',
              }
            } else if (size == 12) {
              return {
                name: 'XL',
              }
            } else {
              return {
                name: size,
              }
            }
          })
      }

      // Process SKUs for this variant
      if (colorSizes && colorSizes.length > 0) {
        for (const variantSize of colorSizes) {
          const isInStock = true
          const size = variantSize.name || ''

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
            operation_type: 'INSERT',
            variant_id: uuidv5(
              `${productId}-${colorName}-${size}`,
              '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
            ),
            variant_description: '',
          }

          console.log(formattedVariant, 'formatted variant')
          formattedProduct.variants.push(formattedVariant)
        }
      }
    }
  }

  // Remove duplicate variants based on variant_id
  const uniqueVariants = []
  const seenVariantIds = new Set()

  for (const variant of formattedProduct.variants) {
    if (!seenVariantIds.has(variant.variant_id)) {
      seenVariantIds.add(variant.variant_id)
      uniqueVariants.push(variant)
    } else {
      console.log(
        `Removed duplicate variant with ID: ${variant.variant_id} for product: ${formattedProduct.name}`
      )
    }
  }

  formattedProduct.variants = uniqueVariants
  console.log(
    `Product ${formattedProduct.name}: ${uniqueVariants.length} unique variants after deduplication`
  )

  const mongoResult = await saveProductToMongoDB(formattedProduct)

  return { formattedProduct, mongoResult }
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
    // Create new store entry
    const newStore = new Store({
      products: productIds,
      name: storeData.name || 'Lululemon',
      storeTemplate: 'lululemon-template',
      storeType: 'lululemon',
      storeUrl: 'https://shop.lululemon.com',
      city: '',
      state: '',
      country: storeData.country || 'US',
      isScrapped: true,
      returnPolicy:
        storeData.returnPolicy ||
        'https://shop.lululemon.com/help/returns-and-refunds',
      tags: ['women', 'men', 'athleisure', 'yoga', 'fitness'],
    })

    await newStore.save()
    console.log(`✅ Created new store with ${productIds.length} products`)
    return { operation: 'CREATED', store: newStore }
  } catch (error) {
    console.error('❌ Error saving store entry:', error.message)
    return { operation: 'ERROR', error: error.message }
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

  // Remove duplicate products based on productId
  console.log(`\n🔍 Removing duplicate products based on product IDs...`)
  console.log(`Original product count: ${products.length}`)

  const uniqueProducts = []
  const seenProductIds = new Set()
  let duplicateCount = 0

  for (const product of products) {
    const productId = product.productId?.toString()

    if (!productId) {
      console.log(
        `⚠️ Product without ID found: ${
          product.displayName || 'Unknown'
        }, skipping...`
      )
      continue
    }

    if (!seenProductIds.has(productId)) {
      seenProductIds.add(productId)
      uniqueProducts.push(product)
    } else {
      duplicateCount++
      console.log(
        `🗑️ Removed duplicate product: ${
          product.displayName
        } (ID: ${productId}) from ${product._category || 'Unknown'} category`
      )
    }
  }

  console.log(`✅ Deduplication completed:`)
  console.log(`   Original products: ${products.length}`)
  console.log(`   Unique products: ${uniqueProducts.length}`)
  console.log(`   Duplicates removed: ${duplicateCount}`)

  // Process products sequentially to avoid overwhelming the API
  console.log(
    `\n📦 Processing ${uniqueProducts.length} unique products from all categories sequentially...`
  )

  for (let i = 0; i < uniqueProducts.length; i++) {
    const product = uniqueProducts[i]
    const gender = product._gender || 'Women'
    const category = product._category || 'Unknown'

    console.log(
      `Processing ${category} product ${i + 1}/${uniqueProducts.length}: ${
        product.displayName
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

      // Add a small delay between products to be respectful
      await new Promise((resolve) => setTimeout(resolve, 500))
    } catch (error) {
      console.error(
        `Error processing product ${product.displayName}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
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

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog.json')
  const catalogData = {
    store_info: {
      name: storeData.name || 'Lululemon',
      domain: 'shop.lululemon.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ['Women', 'Men'],
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

async function scrapeLululemonProducts() {
  try {
    // Connect to MongoDB
    await connectDB()

    console.log('🚀 Starting Lululemon scraping...')

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
    const allResults = []

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
    console.log('🎯 PROCESSING ALL PRODUCTS TOGETHER 🎯')
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
    console.log('🎉 ALL LULULEMON SCRAPING COMPLETED SUCCESSFULLY! 🎉')
    console.log(`${'🎉'.repeat(20)}`)

    // Summary for combined results
    const combinedResult = allResults[0] // Only one result since we combined everything
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
    // Disconnect from MongoDB
    await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeLululemonProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 Lululemon products crawling completed successfully!')

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
      console.log('\n❌ Lululemon crawling failed')
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

module.exports = { main, scrapeLululemonProducts }
