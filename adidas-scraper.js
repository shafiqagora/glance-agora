// Adidas Products Scraper - Men's and Women's Categories
// Uses fetch API to scrape products from Adidas.com
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
  retryPuppeteerWithProxyRotation,
  retryRequestWithProxyRotation,
} = require('./utils/helper')
const puppeteer = require('puppeteer-extra')
const StealthPlugin = require('puppeteer-extra-plugin-stealth')
puppeteer.use(StealthPlugin())
// Global browser instance for persistent session
let globalBrowser = null
let globalPage = null

// Initialize persistent browser instance
async function initializeBrowser(country = 'US') {
  if (globalBrowser) {
    console.log('Browser already initialized')
    return { browser: globalBrowser, page: globalPage }
  }

  console.log('🚀 Initializing persistent browser instance...')

  // Determine proxy endpoint based on country
  let endpoint
  if (country === 'US') {
    endpoint = 'us.decodo.com'
  } else if (country === 'IN') {
    endpoint = 'in.decodo.com'
  } else {
    endpoint = 'us.decodo.com' // Default to US
  }

  const proxyConfig = {
    username: process.env.PROXY_USERNAME || 'splmzpsd06',
    password: process.env.PROXY_PASSWORD || 'es7s2W=dDbn6rGy4En',
    endpoint: endpoint,
    port: process.env.PROXY_PORT || 10000,
  }

  // Create proxy URL for Puppeteer
  const proxyUrl = `http://${proxyConfig.username}:${proxyConfig.password}@${proxyConfig.endpoint}:${proxyConfig.port}`

  console.log(`🌍 Using ${country} proxy endpoint: ${proxyConfig.endpoint}`)

  // Launch browser with proxy and stealth settings
  globalBrowser = await puppeteer.launch({
    headless: false,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      `--proxy-server=${proxyUrl}`,
    ],
    ignoreDefaultArgs: ['--enable-automation'],
  })

  globalPage = await globalBrowser.newPage()

  // Set realistic viewport
  await globalPage.setViewport({
    width: 1366,
    height: 768,
    deviceScaleFactor: 1,
  })

  // Set user agent and headers
  await globalPage.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  )
  // Navigate to Adidas.com first to establish session with human-like behavior
  console.log('🏠 Navigating to Adidas.com to establish session...')

  try {
    // First navigate to the main page
    await globalPage.goto('https://www.adidas.com/us', {
      waitUntil: 'networkidle0',
      timeout: 30000,
    })

    // Wait a bit to let the page fully load
    await new Promise((resolve) => setTimeout(resolve, 2000))

    // Check if we got blocked
    const pageContent = await globalPage.content()
    if (
      pageContent.includes('unable to serve') ||
      pageContent.includes('blocked') ||
      pageContent.includes('Access Denied')
    ) {
      console.log('⚠️ Detected blocking page, trying to handle...')

      // Try refreshing the page
      await globalPage.reload({ waitUntil: 'networkidle0' })
      await new Promise((resolve) => setTimeout(resolve, 3000))

      // Check again
      const newContent = await globalPage.content()
      if (
        newContent.includes('unable to serve') ||
        newContent.includes('blocked') ||
        newContent.includes('Access Denied')
      ) {
        console.log(
          '❌ Still blocked after refresh. You may need to solve CAPTCHA manually or try a different proxy.'
        )
        // Don't throw error, let's continue and see if API calls work
      }
    }

    console.log('✅ Browser initialized and session established')
    return { browser: globalBrowser, page: globalPage }
  } catch (error) {
    console.error('Error during browser initialization:', error.message)
    throw error
  }
}

// Clean up browser instance
async function closeBrowser() {
  if (globalBrowser) {
    console.log('🧹 Closing browser instance...')
    await globalBrowser.close()
    globalBrowser = null
    globalPage = null
    console.log('✅ Browser closed')
  }
}

// Function to fetch detailed product information from Adidas API
const fetchProductDetails = async (productId) => {
  console.log(`Fetching detailed product info for: ${productId}`)

  try {
    const apiUrl = `https://www.adidas.com/api/products/${productId}?sitePath=us`
    console.log(apiUrl)

    // Ensure browser is initialized
    if (!globalPage) {
      await initializeBrowser('US')
    }

    // Navigate to the API URL and get the response
    const response = await globalPage.goto(apiUrl, {
      waitUntil: 'networkidle0',
    })
    const responseText = await response.text()

    return JSON.parse(responseText)
  } catch (error) {
    console.error(
      `Error fetching product details for ${productId}:`,
      error.message
    )
    return null
  }
}

// Helper function to process a single product
const processProduct = async (product) => {
  console.log(`Processing product: ${product.title}`)

  const productId = product.id

  // Fetch detailed product information from API
  const productDetails = await fetchProductDetails(productId)

  const formattedProduct = {
    parent_product_id: productId,
    name: product.title,
    description: productDetails.product_description?.text || '',
    category: product._category || 'Unknown',
    retailer_domain: 'adidas.com',
    brand: 'Adidas',
    gender: product._gender || 'Unisex',
    materials: '',
    return_policy_link:
      'https://www.adidas.com/us/help/us-returns-refunds/how-do-i-return-my-products',
    return_policy: '',
    size_chart: '',
    available_bank_offers: '',
    available_coupons: '',
    variants: [],
    operation_type: 'INSERT',
    source: 'adidas',
  }

  const colors = [
    {
      name: 'Default',
      search_color: productDetails.attribute_list.color,
      price_information: productDetails.pricing_information,
      image_url: productDetails?.product_listing_assets?.[0]?.image_url || '',
      altImage: productDetails?.product_listing_assets?.[1]?.image_url || '',
      url: productDetails?.meta_data.canonical.replace('//', 'https://'),
      productId: productDetails.id,
    },
    ...productDetails.product_link_list,
  ]

  const sizes = productDetails.variation_list

  for (size of sizes) {
    const sizeId = size.id
    try {
      const apiUrl = `https://www.adidas.com/api/skus/${size.sku}/variations/availability`

      // Ensure browser is initialized
      if (!globalPage) {
        await initializeBrowser('US')
      }

      // Navigate to the API URL and get the response
      const pageResponse = await globalPage.goto(apiUrl, {
        waitUntil: 'networkidle0',
      })
      const responseText = await pageResponse.text()

      const response = { data: JSON.parse(responseText) }

      colors = colors.map((color) => {
        const sizeAvailability = response.data.lines.find(
          (item) => item.itemId == color.productId
        )
        if (sizeAvailability.inStock)
          return {
            ...color,
            sizes: [...color.sizes, size.size],
          }
        else return color
      })
    } catch (error) {
      console.error(
        `Error fetching product details for ${productId}:`,
        error.message
      )
      return null
    }
  }

  colors.map((variant) => {
    const originalPrice = variant.price_information.find(
      (item) => item.type == 'original'
    ).value
    const salePrice = variant.price_information.find(
      (item) => item.type == 'sale'
    ).value
    const finalPrice = salePrice > 0 ? salePrice : originalPrice
    const discount = calculateDiscount(originalPrice, salePrice)
    const isOnSale = salePrice > 0 && salePrice < originalPrice

    // Since we don't have size/color details, we'll create a base variant

    variant.sizes.map((size) => {
      const formattedVariant = {
        price_currency: 'USD',
        original_price: originalPrice,
        link_url: `https://www.adidas.com${product.url}`,
        deeplink_url: `https://www.adidas.com${product.url}`,
        image_url: product.image_url || '',
        alternate_image_urls: product.altImage,
        is_on_sale: isOnSale,
        is_in_stock: true, // Assuming in stock since we don't have availability data
        size: size, // No size info available from list API
        color: variant.search_color, // No specific color info from list API
        mpn: uuidv5(
          `${productId}-${variant.search_color}`,
          '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
        ),
        ratings_count: 0,
        average_ratings: 0,
        review_count: 0,
        selling_price: originalPrice,
        sale_price: isOnSale ? salePrice : 0,
        final_price: finalPrice,
        discount: discount,
        operation_type: 'INSERT',
        variant_id: uuidv5(
          `${productId}-${variant.search_color}-${size}`,
          '6ba7b810-9dad-11d1-80b4-00c04fd430c1'
        ),
        variant_description: product.subTitle || '',
      }

      formattedProduct.variants.push(formattedVariant)
    })
  })

  // Save to MongoDB (uncomment when ready)
  // const mongoResult = await saveProductToMongoDB(formattedProduct)

  return {
    formattedProduct,
    mongoResult: { success: true },
  }
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
      storeType: 'adidas',
      name: 'Adidas',
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
        name: storeData.name || 'Adidas',
        storeTemplate: 'adidas-template',
        storeType: 'adidas',
        storeUrl: 'https://www.adidas.com',
        city: '',
        state: '',
        country: storeData.country || 'US',
        isScrapped: true,
        returnPolicy:
          'https://www.adidas.com/us/help/us-returns-refunds/how-do-i-return-my-products',
        tags: ['men', 'women', 'sports', 'clothing', 'footwear'],
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
async function scrapeAdidasCategory(categoryConfig, targetProductCount = 2500) {
  let start = 0
  let allProducts = []
  let hasMoreProducts = true
  const pageSize = 48 // Adidas typically uses 48 products per page

  console.log(`\n🎯 Starting to scrape ${categoryConfig.name} category...`)
  console.log(`Target: ${targetProductCount} products`)

  while (hasMoreProducts && allProducts.length < targetProductCount) {
    console.log(`\nFetching ${categoryConfig.name} starting from ${start}...`)

    try {
      // Build the API URL
      const apiUrl = `https://www.adidas.com/plp-app/_next/data/_kQr4GNn-lJ6gIOcC3G70/us/${categoryConfig.url
        .split('/')
        .pop()}.json?start=${start}&path=us&taxonomy=${categoryConfig.url
        .split('/')
        .pop()}`

      console.log(`API URL: ${apiUrl}`)

      // Fetch data from API
      console.log(`Fetching: ${apiUrl}`)
      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          return await axiosInstance.get(apiUrl, {
            headers: {
              'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              Accept: 'application/json, text/plain, */*',
              'Accept-Language': 'en-US,en;q=0.9',
              'Accept-Encoding': 'gzip, deflate, br',
              Cookie: `x-browser-id=74b1cf01-5659-45e7-996d-4057434e264e; _fbp=fb.1.1752782012368.368694604995087212; _pin_unauth=dWlkPU5qWmxZV0ppWkdFdE1XWTVaUzAwWkRjM0xUZzBNMlF0TVRoa05UaGpPR0k0TnpWaQ; mt.v=1.738118099.1752782000323; _ga=GA1.1.2099791898.1752782014; _scid=xSdakCa3lx1_fuV444i7Y9c8FhBM6OE5; _tt_enable_cookie=1; _ttp=01K0CZ64GMQH6RNK4WSZY28KB4_.tt.1; QuantumMetricUserID=3b42ec9ff5b933e53d943e0759d79fcf; forterToken=0a8d38d9083d4463a2fe6c66c64f7735_1756189304316__UDF43-mnts-a9-r9-s6_17ck_zEdeCTrv9/E%3D-173-v2; badab=false; channelflow=nonpaid|other|1761455327615; channeloriginator=nonpaid; channelcloser=nonpaid; akacd_Phased_www_adidas_com_Generic=3937971511~rv=58~id=d03616f0c0a1d1b49358bafa2532bf07; geo_ip=154.198.69.35; geo_country=PK; geo_state=; onesite_country=US; akacd_phased_PDP=3937971514~rv=85~id=c5ce40b32c18048e38e8c738bbe85d55; x-original-host=www.adidas.com; x-environment=production; gl-feat-enable=CHECKOUT_PAGES_ENABLED; x-session-id=47ad3b47-2766-4a83-91d1-9e24a8ddfe76; x-browser-id-cs=74b1cf01-5659-45e7-996d-4057434e264e; x-commerce-next-id=5b1e3e56-e6ee-4a5f-af48-3917c4e260ed; mt.v=1.738118099.1752782000323; forterToken=0a8d38d9083d4463a2fe6c66c64f7735_1756189304316__UDF43-mnts-a9-r9-s6_17ck_zEdeCTrv9/E%3D-173-v2; x-commerce-next-id=5b1e3e56-e6ee-4a5f-af48-3917c4e260ed; ab_qm=b; AMCVS_7ADA401053CCF9130A490D4C%40AdobeOrg=1; s_cc=true; akacd_phased_PLP=3937971642~rv=91~id=5da198ea98a1c474db9a0ebdacacfd17; x-site-locale=en_US; _gcl_au=1.1.1289107017.1760591175; newsletterShownOnVisit=true; geo_postcode=; geo_coordinates=lat=33.60, long=73.07; AMCV_7ADA401053CCF9130A490D4C%40AdobeOrg=-227196251%7CMCIDTS%7C20385%7CMCMID%7C90408032731919581470279506950832521089%7CMCAAMLH-1761800883%7C3%7CMCAAMB-1761800883%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1761203283s%7CNONE%7CMCAID%7CNONE; _ScCbts=%5B%2296%3Bchrome.2%3A2%3A5%22%2C%22572%3Bchrome.2%3A2%3A5%22%5D; _sctr=1%7C1761159600000; QSI_SI_1HBh4b3ZpUvgHMV_intercept=true; mt.sc=%7B%22i%22%3A1761198949869%2C%22d%22%3A%5B%5D%7D; QuantumMetricSessionID=e5e283d4c70e04a042c4eb4ec7dc0203; tfpsi=8ba0fc6b-d506-4185-863f-a20bafc2f296; __olapicU=6dd3e0c62c1b3bff64727171c0f1b849; akacd_api_3stripes=3938651754~rv=14~id=e74ba2059fafb2838c4dd15767e32d21; bm_ss=ab8e18ef4e; AKA_A2=A; adidas_country=us; s_sess=%5B%5BB%5D%5D; bm_so=93AE2E8547B45D1102E0AF1B78323AA7F35060336DEEC9467ADE17823A6B04E1~YAAQU84tF0F9Y+iZAQAAkYXADwWEVpbZzBZUyIJDrxXydWyEHesCixNm6HcDZWVI4V8wpRXwXfXJh0adp662c4Y3u7tmFrt1BE1KwYeUlLJIyu6Wru6pWTOCeVo9hua0n0dYsRx8q26WQuf37pM8HJCFMv7Jqp/qVSxC288kZLMvR6E/V8vigSRrjNnWnzfbR5JNyM+xh1+4xkhbdpmOnFk6BmDQAE2mW/D4Ax8v42e4hPuOPiykgZkyQgv6UfALS708dgCE41kh+nexmMWvVWT2qC2N4C5ijF8UQYhdeB3HTrFcLJcp4+1iecYOfOCFTqxZQnfqdSQL9cVu7JbxPw51hfR4VfQ+4E2IlwqNgy1iGQbc9MQYDDMbRHyD2RBu5zDaA47NrRdofKcDMFw3DZmWYXKVpASwrKtGdBdo3kW4+b5V7TzpZaDese0gDqgfx4FeY/I1lbzW6suJ4Ok=; bm_sz=3B9D3C8DEFB1D8366F4320127EDA76D0~YAAQU84tF0J9Y+iZAQAAkYXADx2ZDsBhB9yO4U9aLFFF0wzEXEDUI5ph/0kyuhvv4Qgf8znCq48TiYQwIYPUJBb8U3gTmSHwJHIb+lPHH4kG1gfZAZiVeqZ63JxwHDUV7SKbQgH2iioglIwM8nHaW8pRIxuQHxfcW6VcwO0VI4hHaJFDAPTW0IzbzXYNkbod5vZnsp1H2RCSanXpob7MkJSoMehItqiE2c0fQSxuzBRP6w51rcvDFMA058DoEbqB9uzBfRKMK1AILcX6bz1sBI6FOHH2+AEjpKwT6It51CLaK3WbiAz7GbZYiPfzHRmxSstXgZlb3PdxHCswG9dy2RD/Dk5t5h1agOuPGH6FmCWy8WZhSTYEpEl5n7j7p1mUyeAVrD64s6r/+U8qoQD0kb+pFdOQafC5CbPGrmlvbBqh4mO60e81Z3A/aihPgSd8RBwyWjRIGUH4Q7ZkpT+L6l4ZnSnSrAoVBrsMUQwSnpfnwKJ5/MlWC+j9Blwp+JcrXFEU6E2ge2nKyQ3K7dmroae9NH2rD48e8ZH2kgrABfDXnw==~3551812~4408626; UserSignUpAndSave=2; RT="z=1&dm=adidas.com&si=47857bc4-f6c6-48be-87e8-3abcf5a73a23&ss=mh30gxyy&sl=g&tt=w15&bcn=%2F%2F684d0d43.akstat.io%2F&ld=a1o&nu=2rnf43tv&cl=2l6o&hd=11vqs"; ak_bmsc=553FC0A48DF3C055CA62828C38175FF5~000000000000000000000000000000~YAAQU84tF719Y+iZAQAA5IzADx2I9cndsTb4VH1dHZG4qN/blcjJaBAnHer68rxz9vqNeQG6PlAzqT3czOlWyqMTi0SIwt9G1bgDCfYpPR9rSfvDMRRm+cZkSjFps5roq7cah7+9b0cp+X4NNL8M/11FLqbPHE0YJR2o7yvKq6rnvJscbOie2F/O7lr5quZcKMMG0paUzLsT27XIk11orAyphRWbqXkfAIPOS3t2K5ZcpZ3iol2XZKndhes8XfZh11sy/GaYHOs8fcy5pSjootzT3aNqTL7gh/sXjhZ9Uo5yq8LDRH7on1T7EtPHqVo25y0u+GMPM/XUO+ExAjovPJ/LCeo/ozCwcm3tAtN1JtlUMj0JQNF78Q7Dlm1OVPvauSAHZJVhZjVe0pdb; _rdt_uuid=1752782011423.5ba8d148-6c63-47b2-90fd-486a95490dc6; bm_lso=93AE2E8547B45D1102E0AF1B78323AA7F35060336DEEC9467ADE17823A6B04E1~YAAQU84tF0F9Y+iZAQAAkYXADwWEVpbZzBZUyIJDrxXydWyEHesCixNm6HcDZWVI4V8wpRXwXfXJh0adp662c4Y3u7tmFrt1BE1KwYeUlLJIyu6Wru6pWTOCeVo9hua0n0dYsRx8q26WQuf37pM8HJCFMv7Jqp/qVSxC288kZLMvR6E/V8vigSRrjNnWnzfbR5JNyM+xh1+4xkhbdpmOnFk6BmDQAE2mW/D4Ax8v42e4hPuOPiykgZkyQgv6UfALS708dgCE41kh+nexmMWvVWT2qC2N4C5ijF8UQYhdeB3HTrFcLJcp4+1iecYOfOCFTqxZQnfqdSQL9cVu7JbxPw51hfR4VfQ+4E2IlwqNgy1iGQbc9MQYDDMbRHyD2RBu5zDaA47NrRdofKcDMFw3DZmWYXKVpASwrKtGdBdo3kW4+b5V7TzpZaDese0gDqgfx4FeY/I1lbzW6suJ4Ok=^1761200869297; _uetsid=42166dd0afce11f0a8a0af89183cc851; _uetvid=ba8d42b0634711f0abf1379c71a59772; _ga_4DGGV4HV95=GS2.1.s1761198950$o15$g1$t1761200869$j24$l0$h0; utag_main=v_id:019819f2e6df000b34cbd924604805075001906d00d57$_sn:14$_se:42%3Bexp-session$_ss:0%3Bexp-session$_st:1761202669617%3Bexp-session$ses_id:1761198942679%3Bexp-session$_pn:9%3Bexp-session$_vpn:121%3Bexp-session$ttdsyncran:1%3Bexp-session$dc_visit:1$dc_event:110%3Bexp-session$dcsyncran:1%3Bexp-session$ttd_uuid:2ddabf2f-00cd-4c05-897e-fb5112f687de%3Bexp-session$cms_514:1%3Bexp-session$_prevpage:PLP%7CG_MEN%7CPT_PANTS%3Bexp-1761204469192; s_pers=%20s_vnum%3D1761937200340%2526vn%253D4%7C1761937200340%3B%20pn%3D4%7C1763792865335%3B%20s_invisit%3Dtrue%7C1761202669631%3B; _scid_r=16dakCa3lx1_fuV444i7Y9c8FhBM6OE5rnDI5w; ttcsid_CBTS7IRC77UFL42ENA4G=1761198950517::G8lrOnjmM-R-bRnw4c6m.15.1761200869697.0; ttcsid=1761198950517::qFaSADVUrDz-5gSkN9pN.15.1761200869697.0; _derived_epik=dj0yJnU9dG9USHdKa0xkWDNWQTdNNDhJU0ZGUFNLZXN0SDdwTEgmbj00SUpibjNGb0p3WExSSU9fQWN0cDRBJm09MSZ0PUFBQUFBR2o1eXVZJnJtPTEmcnQ9QUFBQUFHajV5dVkmc3A9NQ; bm_sv=95D3CCAB7340168A7D44FBBEF7BA0C8B~YAAQTs4tF2QUxOyZAQAAjZrADx0jkLWAaBG/Q3qMieSgGvNSFVydy3vTk0Qub06tbzxiiZlpTIA8bBLs1+dZPB4A2WHHQiyqDkshNV+SR+fovG9k6GRW6auxBHiW0W4B5ZsBh2Y6GPE+QXhHsRqSh8Hbg6gkeBqlEvJrQRwOc5IuCC2RozSnGkUindbZ2rTJrlLE/63+jx+Q9Dz0/o9adN5F/gVEJHwtSuIBbz6yfyDHZCHL5jG83Hof8PBiJkWM~1; _abck=46F978DC0417D7D4B1D8B8C0EB460AE2~-1~YAAQU84tFw1/Y+iZAQAAMp3ADw75A1UHgRhLtSKpy46HH/akAiZY9Is4d8qmtCumrHuIahdPcnZj7iuwsz4a+FhcJ6oPLJewkHA71Kr1FifvDpNXQCy7SvAUn3zlgkVFnKyB2tAMOH0ne3xOjD/G/4R8/6D53EwFGP/DuO/GapWbNm8qhzj5y7683e+Yi4X+P/yONuXzoe3VoiT87VqRrBaTJJa7CzI2FyTYVWwro91/SOCtw6SEXM2zv8SRVq+iPoCX5z9O+wX8Lpdaw2Dt/Ew+1hjrY8Y9nhXGMyVhl6xk5eNKihau2BgFm4RzatFWxhDNBdi9U1VXxmJCDDhHE6bZdnNb9M7+gknt83wPQKX2fNITXFkRJ9ydfIZSqoMK6FOW2w0RdkdsOM3l4K3dMOIRB9BQ766kAeG0GUrKU/0EuCWGNp/vg79nKnx/m6LHPQ19fZbf2NWGE9yBeUgjNctLPkz7Z1kTa7LxOSUwvMP/tercOgKImKynL3KoNuPK7ZChG5g2TVF7Of5Fz8CeaNIfiFPyXxLcDTwKLUEsI4w4N0bVUUpa8WzOB7MGQ1vAsGixJNfc3sGgITUK+e2akmNcqj1untxq5n9b1qdrJa/a3JqSu2sY5AXvmws5N4eX68tc8yKzIqCPWgmHLhlp7LnWHUx8KYMfsjgnaiaSxy5kWSVoJxS8kQAK5/rLXZDvIKlZWQTWwS+d0EOId5XakddQtcyl1P70ppHwilR7fLGa460FZT30PnF3SsDqeID2KmlB6Jzk75bg/6IxZW7BJaSLecnNNPmG7yxnK/vSCpiiRa4yZ0Egojo=~-1~-1~1761203229~AAQAAAAE%2f%2f%2f%2f%2f%2fcxtM8JDKfcuU3yeXJtWaNStN+UzUfu9zAZ7F11A4xHecblmo1LXfdRbQ6HHGQuPH+bHbh2kN1s7PPGKUcqardH1HK5g4yN9akiqBxUkME2ty1hpDrjX4lgdvyvK6NN06JTAR%2frr1NXBuRqxi0+uo4937Dh7GO4wFjZR4EDOOry1poTOzjYarDm2cbKkvRkYtyxWXmvK8k4~1761200932; bm_s=YAAQU84tF++DY+iZAQAATNLADwRge5YyPmV5ZhNd1KiNSFvM058a6VEE00ADx0DZ7J9qwSs//rQFDdMTXWiwATEIOgK4+nFk4vz8Skr6TiF1DcH8IIq01FiBeXOqR54Cth1T6rTlYuAl6uOHUmmGKwjeKbqCCpKaL6IvvKxsHbBe6Y0BEvMuH4dSsKeU6X+qmNerCa8f1UzZdSuRDAGikwt4Bm1J0yICPt2EsHX7Mmdw9AD3/+Cq2XlUjXsxKn0Aj/Xu/pk0Ipakp8gbZyuBKc9jYNINCv3YGTCMz0Am4mYZdKbK43H4ep54iGGfhPepEJSbGdlQeHFNUzMngU5e8/9q9fNji5FhmxMInAvqhZ98HfMtqHRJVjRGy9OkD0p2xvLKMKCtGSQUpRm1LEagYPPEzqXsKwIRh7vyb5pic/bYv6hIZwmL5KDMl+hFe6cuf9Uac42aqu0QNIiNCorPp6nDuahMtJPMUksufd6yvmoyHgWIYAjoc3cQMWlMFPNi60h5bN+pom2zDvrAlUmvvZV7Y3VwU3CBx48c0tiikNv6FW8s9MHFeRDcK4JV13JhZjcGpQUuwxuu; RT="z=1&dm=adidas.com&si=47857bc4-f6c6-48be-87e8-3abcf5a73a23&ss=mh30gxyy&sl=g&tt=w15&bcn=%2F%2F684d0d43.akstat.io%2F&ld=a1o&nu=w3r6cte&cl=12cgd&hd=11vqs"`,
            },
          })
        },
        5, // maxRetries
        2000, // baseDelay
        'US' // country
      )

      const pageData = response.data

      // Extract products from the response
      let pageProducts = []
      if (pageData?.pageProps?.products?.length > 0) {
        pageProducts = pageData.pageProps.products
        console.log(
          `Found ${pageProducts.length} products on ${categoryConfig.name} page starting from ${start}`
        )
        allProducts = allProducts.concat(pageProducts)
        console.log(
          `${categoryConfig.name} total products so far: ${allProducts.length}`
        )

        // Check if we have more products
        if (pageProducts.length < pageSize) {
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
    const gender = product._gender || 'Unisex'
    const category = product._category || 'Unknown'

    console.log(
      `Processing ${category} product ${i + 1}/${products.length}: ${
        product.displayName || product.name
      }`
    )

    try {
      const result = await processProduct(product)

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
      await new Promise((resolve) => setTimeout(resolve, 100))
    } catch (error) {
      console.error(
        `Error processing product ${product.displayName || product.name}:`,
        error.message
      )
      mongoResults.errors++
    }
  }

  // Create directory structure: countryCode/retailername-countryCode-combined/
  const cleanBrandName = 'adidas'
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
      name: storeData.name || 'Adidas',
      domain: 'adidas.com',
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

async function scrapeAdidasProducts() {
  try {
    // Connect to MongoDB
    // await connectDB()

    console.log('🚀 Starting Adidas scraping using fetch API...')

    // Define categories to scrape
    // Note: You'll need to get the correct buildId and paths from Adidas website
    const categories = [
      {
        name: 'Men Pants',
        gender: 'Men',
        url: 'https://www.adidas.com/us/men-pants',
      },
      // {
      //   name: 'Men Shorts',
      //   gender: 'Men',
      //   url: 'https://www.adidas.com/us/men-shorts',
      // },
      // {
      //   name: 'Men Tops',
      //   gender: 'Men',
      //   url: 'https://www.adidas.com/us/men-tops',
      // },
      // {
      //   name: 'Men Jerseys',
      //   gender: 'Men',
      //   url: 'https://www.adidas.com/us/men-jerseys',
      // },
      // {
      //   name: 'Men Tracksuits',
      //   gender: 'Men',
      //   url: 'https://www.adidas.com/us/men-track_suits',
      // },
      // {
      //   name: "Men's Hoodies & Sweatshirts",
      //   gender: 'Men',
      //   url: 'https://www.adidas.com/us/men-hoodies_sweatshirts',
      // },
      // {
      //   name: 'Men Jackets',
      //   gender: 'Men',
      //   url: 'https://www.adidas.com/us/men-jackets',
      // },
      // {
      //   name: 'Women Jackets',
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-jackets',
      // },
      // {
      //   name: 'Women Pants',
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-pants',
      // },
      // {
      //   name: "Women's Skirts & Dresses",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-skirts_dresses',
      // },
      // {
      //   name: "Women's Tights & Leggings",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-tights_leggings',
      // },
      // {
      //   name: "Women's Plus Size Shoes & Clothes",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-plus_size',
      // },
      // {
      //   name: "Women's Tracksuits",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-track_suits',
      // },
      // {
      //   name: "Women's Tops",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-tops',
      // },
      // {
      //   name: "Women's Shorts",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-shorts',
      // },
      // {
      //   name: "Women's Sports Bras",
      //   gender: 'Women',
      //   url: 'https://www.adidas.com/us/women-sports_bras',
      // },
    ]

    const targetProductsPerCategory = 5
    const allResults = []

    const storeData = {
      name: 'Adidas',
      domain: 'adidas.com',
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

      const categoryProducts = await scrapeAdidasCategory(
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
    console.log('🎉 ALL ADIDAS SCRAPING COMPLETED SUCCESSFULLY! 🎉')
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
    await disconnectDB()
  }
}

// Export the main function for use in other modules
const main = async () => {
  try {
    const results = await scrapeAdidasProducts()

    if (results && results.length > 0) {
      console.log('\n🎉 Adidas products crawling completed successfully!')

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
      console.log('\n❌ Adidas crawling failed')
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

module.exports = { main, scrapeAdidasProducts }
