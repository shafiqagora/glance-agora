// Abercrombie & Fitch Products Scraper
// Uses their GraphQL API to scrape products from different categories
require('dotenv').config()
const fs = require('fs')
const sanitizeHtml = require('sanitize-html')
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

// Helper function to process a single product
const processProduct = async (
  product,
  categoryGender = 'Unisex',
  categoryName = 'Unknown'
) => {
  console.log(`Processing product: ${product.name}`)

  const productId = product.id

  // Clean and format product name
  const productName = product.name || ''

  // Extract gender from product data or use category gender
  const gender = categoryGender

  // Build base product URL
  const baseUrl = 'https://www.abercrombie.com'
  const productUrl = `${baseUrl}${product.productPageUrl}`
  // Get main product image
  const imageUrl = product.imageSet?.primaryFaceOutImage
    ? `https://img.abercrombie.com/is/image/anf/${product.imageSet.primaryFaceOutImage}`
    : ''
  const hoverImage = product.imageSet?.primaryHoverImage
    ? `https://img.abercrombie.com/is/image/anf/${product.imageSet.primaryHoverImage}`
    : ''
  const modelImage = product.imageSet?.modelImage
    ? `https://img.abercrombie.com/is/image/anf/${product.imageSet.modelImage}`
    : ''

  const alternateImages = [hoverImage, modelImage].filter(
    (img) => img && img !== imageUrl
  )

  // Build description from short descriptors
  const description = ''

  const formattedProduct = {
    parent_product_id: productId,
    name: productName,
    description: description,
    category: categoryName,
    retailer_domain: 'abercrombie.com',
    brand: 'Abercrombie & Fitch',
    gender: gender,
    materials: '',
    return_policy_link:
      'https://www.abercrombie.com/shop/wd/help/online-return-exchange-policy?originalStore=us',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'abercrombie',
  }
  // Parse price information
  const priceDesc = product.price.description || '$0'
  const originalPrice = parseFloat(
    product.price.originalPrice?.replace('$', '') || '0'
  )
  const discountPrice = product.price.discountPrice
    ? parseFloat(product.price.discountPrice.replace('$', '') || '0')
    : 0
  const finalPrice = discountPrice > 0 ? discountPrice : originalPrice
  const discount = calculateDiscount(originalPrice, discountPrice)
  const isOnSale = discountPrice > 0 && discountPrice < originalPrice

  // Process color variants from swatchList
  if (product.swatchList && product.swatchList.length > 0) {
    for (const swatch of product.swatchList) {
      const colorName = swatch.name || 'Default'
      const variantProductId = swatch.product?.id || productId

      const variantImageUrl = swatch.product?.imageSet?.primaryFaceOutImage
        ? `https://img.abercrombie.com/is/image/anf/${swatch.product.imageSet.primaryFaceOutImage}`
        : imageUrl

      // For Abercrombie, we don't have size data in the listing API
      // So we'll create a single variant per color without specific size info
      const formattedVariant = {
        price_currency: 'USD',
        original_price: originalPrice,
        link_url: productUrl,
        deeplink_url: productUrl,
        image_url: variantImageUrl || imageUrl,
        alternate_image_urls: alternateImages,
        is_on_sale: isOnSale,
        is_in_stock: true, // Assume in stock since we don't have availability data
        size: '', // No size data available in listing API
        color: colorName,
        mpn: uuidv5(
          `${productId}-${colorName}`,
          '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
        ),
        ratings_count: 0,
        average_ratings: 0,
        review_count: 0,
        selling_price: originalPrice,
        sale_price: discountPrice > 0 ? discountPrice : 0,
        final_price: finalPrice,
        discount: discount,
        operation_type: 'INSERT',
        variant_id: uuidv5(
          `${productId}-${variantProductId}-${colorName}`,
          '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
        ),
        variant_description: '',
      }
      formattedProduct.variants.push(formattedVariant)
    }
  } else {
    // If no swatches, create a single variant with main product data
    const formattedVariant = {
      price_currency: 'USD',
      original_price: originalPrice,
      link_url: productUrl,
      deeplink_url: productUrl,
      image_url: imageUrl,
      alternate_image_urls: alternateImages,
      is_on_sale: isOnSale,
      is_in_stock: true,
      size: '',
      color: 'Default',
      mpn: uuidv5(
        `${productId}-Default`,
        '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
      ),
      ratings_count: 0,
      average_ratings: 0,
      review_count: 0,
      selling_price: originalPrice,
      sale_price: discountPrice > 0 ? discountPrice : 0,
      final_price: finalPrice,
      discount: discount,
      operation_type: 'INSERT',
      variant_id: uuidv5(
        `${productId}-Default`,
        '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
      ),
      variant_description: '',
    }
    formattedProduct.variants.push(formattedVariant)
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
      storeType: 'abercrombie',
      name: 'Abercrombie & Fitch',
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
        name: storeData.name || 'Abercrombie & Fitch',
        storeTemplate: 'abercrombie-template',
        storeType: 'abercrombie',
        storeUrl: 'https://www.abercrombie.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy:
          'https://www.abercrombie.com/shop/wd/help/online-return-exchange-policy?originalStore=us',
        tags: ['men', 'women', 'clothing', 'fashion', 'casual'],
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

// Helper function to scrape products from a specific category
async function scrapeAbercrombieCategory(
  categoryConfig,
  targetProductCount = 2500
) {
  let start = 0
  let allProducts = []
  let hasMoreProducts = true
  const pageSize = 90 // Abercrombie uses 90 products per page

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (hasMoreProducts && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} starting from ${start}...`)

    try {
      // Build the GraphQL API URL based on the curl command
      const variables = {
        categoryId: categoryConfig.categoryId,
        facet: [],
        filter: '',
        requestSocialProofData: true,
        rows: pageSize.toString(),
        sort: '',
        start: start.toString(),
        seqSlot: '1',
        grouped: false,
        isUnifiedCategoryPage: false,
        kicIds: '',
      }

      const extensions = {
        persistedQuery: {
          version: 1,
          sha256Hash:
            '097f956378599742746ed2c31c0fe53c43ba1e458bd11bba5310b4b9c00e788c',
        },
      }

      const apiUrl = `https://www.abercrombie.com/api/bff/catalog?catalogId=10901&storeId=11203&langId=-1&brand=anf&store=a-wd&currency=USD&country=US&urlRoot=/shop/wd&aemContentAuthoring=0&operationName=CATEGORY_PAGE_DYNAMIC_DATA_QUERY&variables=${encodeURIComponent(
        JSON.stringify(variables)
      )}&extensions=${encodeURIComponent(JSON.stringify(extensions))}`

      console.log(`API URL: ${apiUrl}`)

      // Fetch data from API
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          accept: '*/*',
          'accept-language': 'en-US,en;q=0.9',
          'content-type': 'application/json',
          'User-Agent':
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
          'sec-ch-ua':
            '"Not)A;Brand";v="8", "Chromium";v="140", "Google Chrome";v="140"',
          'sec-ch-ua-mobile': '?0',
          'sec-ch-ua-platform': '"macOS"',
          'sec-fetch-dest': 'empty',
          'sec-fetch-mode': 'cors',
          'sec-fetch-site': 'same-origin',
          priority: 'u=1, i',
          referer: `https://www.abercrombie.com/shop/wd/${categoryConfig.url}`,
          Cookie:
            'ANFSession=112031759381697183; ANF_TARGET=db271534-e7d3-4e22-acb6-c29b2cb0eefb.35_0; WC_SESSION_ESTABLISHED=true; WC_PERSISTENT=TJmohdE3RHJiJtch3kyFAsCqVXUGLgKUXkuZRfFdkvY%3D%3B2025-10-02+01%3A08%3A17.257_1759381697178-188776_11203_-1002%2C-1%2CUSD%2Cx14aEc%2BV1DnApZpdvj12Tym%2BqVOODHo3Jk%2BAkmhF503d5xQQI4uwr8vUTnTk0QBp7Q05CVpFEEMWHHNz75ioVQ%3D%3D%2C2025-10-02+01%3A08%3A17.257_11203; WC_AUTHENTICATION_-1002=-1002%2CJdh5JVzqovGDGuZIS9A0yAc11LCiDPi1CWNOoTX%2B9xo%3D; WC_ACTIVEPOINTER=-1%2C11203; WC_USERACTIVITY_-1002=-1002%2C11203%2C0%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2Cnull%2C1244873000%2Cver_null%2CBpGAWQQqmVTAVBfiuCmNrv8P4dq3ULPw97BaoSLzs2Evwse3uW1Umjgya8wuFHn3ccGPw0eZA8xTJG6tegSSd88m4xqyNY9%2B%2BFTY2Qe%2BUTY5OSIh8bYMdWdFQXf0%2FF1Q6gp1Rc7K8wCO0qTls52PH7cuuwVWEtnMY3evw9QYgqRzX6vRJRM2t%2BAOHgKKfM0Lj4k12iIkWh7wb2LZFLWPA3ilCJqDdpbdQPmNagYxN%2BvT5rv5L%2BzbAEey5lXdi0%2Fr; WC_ACTIVITYDATA_-1002=G%2C-1%2CUSD%2C11557%2C10861%2C-2000%2C1759381697178-188776%2C-1%2CUSD%2CDhDihDNs2tSbvo93bIq444pDRUtwpcpegnkLRCCW3wjwBytb3SY1axRlZ2eAzj2EiXgwhfyKSU8hkUSCswEE2qiU7R2IAjW46tKyZKLeC1iJ%2BPRzSLOwpj7abHn5mcRpjOs0gKweVmNtVDWzY7dte84RLTdazXn5Nci2sxLWEaGiLq%2Bs5fRnO%2BTXK9MgG7rNG0KlFiTX4da%2FJA4HV5wnJ2nZzsNyQa2CySioI09v0Qo%3D; rxVisitor=17593816986121CS97FK2ATGOKRT5MJ65UFAN5HTP0HMO; at_check=true; AMCVS_92D21C8B532954A90A490D4D%40AdobeOrg=1; s_ecid=MCMID%7C15058843173010961901960492111804236746; utag_main__sn=1; eventdb_testgroup=a; utag_main_vapi_domain=abercrombie.com; utag_main_v_id=0199a352469d001d7fa609a4c78105075003206d0132e; utag_main_dc_visit=1; s_cc=true; _gcl_au=1.1.1099971033.1759381704; _fbp=fb.1.1759381703883.432700348513002825; TAPID=abercrombie-hco/main>0199a352469d001d7fa609a4c78105075003206d0132e|; QuantumMetricUserID=ac25036eba60669e2313003574cb1b96; OptanonAlertBoxClosed=2025-10-02T05:08:46.528Z; dtCookie=v_4_srv_21_sn_01F44362BD37C35EFAC0E2F2B8C4A7F5_app-3A59fcf8f3ef580f1f_1_app-3Aea7c4b59f27d43eb_1_ol_0_perc_100000_mul_1; uPref={%22cfi%22:%221%22%2C%22cur%22:%22USD%22%2C%22sf%22:%22PK%22%2C%22shipTo%22:%22PK%22%2C%22brnd%22:%22kids%22}; adobe_page_type=category; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Oct+02+2025+10%3A19%3A24+GMT%2B0500+(Pakistan+Standard+Time)&version=202501.1.0&browserGpcFlag=0&isIABGlobal=false&identifierType=Cookie+Unique+Id&hosts=&consentId=0f5edcd5-ad41-4ab5-8653-ed612d68b4ab&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&iType=1&intType=1&geolocation=PK%3BPB&AwaitingReconsent=false; adobe_page_name_persist=anf:kids:category:boys:::::grid; s_sq=afabercrombieglobal%3D%2526pid%253Danf%25253Akids%25253Acategory%25253Agirls%25253A%25253A%25253A%25253A%25253Agrid%2526pidt%253D1%2526oid%253Dfunctionkd%252528%252529%25257B%25257D%2526oidt%253D2%2526ot%253DBUTTON; xSeg=s=CX-1&s2=ER&g=EG&co=EN&p=EK; AMCV_92D21C8B532954A90A490D4D%40AdobeOrg=1585540135%7CMCIDTS%7C20368%7CMCMID%7C15058843173010961901960492111804236746%7CMCAAMLH-1760358891%7C3%7CMCAAMB-1760358891%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1759761291s%7CNONE%7CMCAID%7CNONE%7CvVersion%7C4.4.0; mboxEdgeCluster=38; mbox=PC#226863ee707649968f5aedfda5a99e48.38_0#1822998893|session#de0a4d2276154da6806821084e01dbfe#1759755953; _fs_cd_cp_pRdRgnTnF68pCV2F=Afzcdibev8qsrIqlLMmsecU3DiLvirVy5BsVK95NAdqN_ybZwMhqfn7lemsf0loo7AsvGUBuz-ErHfJPMo_zyhFrKjJsEJbeMnJJ7oojoeGDSLcloxk_A7nkZxU8lYY1AlrEfghXDIwhMWcYvF3jOUhWXw==; rxvt=1759755911237|1759754082153; dtPC=21$554082152_768h24vWUJTUACWIOJHHGJHFCABIWTFUJUCOUKM-0e0',
        },
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const pageData = await response.json()

      // Extract products from the response
      let pageProducts = []
      if (pageData?.data?.category?.products?.length > 0) {
        pageProducts = pageData.data.category.products
        console.log(
          `Found ${pageProducts.length} products on ${categoryConfig.name} page starting from ${start}`
        )

        // Limit to first few products for testing
        allProducts = allProducts.concat(pageProducts)
        console.log(
          `${categoryConfig.name} total products so far: ${allProducts.length}`
        )

        // Check if we have more products
        const pagination = pageData.data.category.pagination
        const currentPage = pagination?.currentPage || 1
        const totalPages = pagination?.totalPages || 1

        if (currentPage >= totalPages || pageProducts.length < pageSize) {
          hasMoreProducts = false
        } else {
          start += pageSize
        }
      } else {
        console.log(
          `No products found on ${categoryConfig.name} page starting from ${start}`
        )
        hasMoreProducts = false
      }

      // Add a small delay to be respectful to the API
      await new Promise((resolve) => setTimeout(resolve, 1000))
    } catch (error) {
      console.error(
        `Error fetching ${categoryConfig.name} page starting from ${start}:`,
        error.message
      )
      hasMoreProducts = false
    }

    // Break for testing - remove this in production
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
    const gender =
      product._gender ||
      (product.gender === 'M'
        ? 'Men'
        : product.gender === 'F'
        ? 'Women'
        : 'Unisex')
    const category = product._category || 'Unknown'

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.name
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
      console.error(`Error processing product ${product.name}:`, error.message)
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'abercrombie'
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
      name: storeData.name || 'Abercrombie & Fitch',
      domain: 'abercrombie.com',
      currency: storeData.currency || 'USD',
      country: countryCode,
      total_products: formattedProducts.length,
      categories: ['Men', 'Women'],
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

async function scrapeAbercrombieProducts() {
  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Starting Abercrombie & Fitch scraping using GraphQL API...')

    // Define categories to scrape
    const categories = [
      {
        name: "Men's Bottoms",
        gender: 'Men',
        categoryId: '6570710', // Men's category
        url: 'mens',
      },
      {
        name: "Men's Activewear",
        gender: 'Men',
        categoryId: '66909816', // Men's category
        url: 'mens',
      },
      {
        name: "Men's Coats & Jackets",
        gender: 'Men',
        categoryId: '12221', // Men's category
        url: 'mens',
      },
      {
        name: "Women's Clothing",
        gender: 'Women',
        categoryId: '12203', // Women's category
        url: 'womens',
      },
      {
        name: "Kids's Clothing",
        gender: 'Kids',
        categoryId: '179206', // Women's category
        url: 'kids',
      },
    ]

    const targetProductsPerCategory = 100
    const allResults = []

    const storeData = {
      name: 'Abercrombie & Fitch',
      domain: 'abercrombie.com',
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

      const categoryProducts = await scrapeAbercrombieCategory(
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
    console.log('🎉 ALL ABERCROMBIE SCRAPING COMPLETED SUCCESSFULLY! 🎉')
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
    // Disconnect from MongoDB
    // await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeAbercrombieProducts()

    if (results && results.length > 0) {
      console.log(
        '\n🎉 Abercrombie & Fitch products crawling completed successfully!'
      )

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
      console.log('\n❌ Abercrombie & Fitch crawling failed')
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

module.exports = { main, scrapeAbercrombieProducts }
