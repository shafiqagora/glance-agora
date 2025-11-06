const puppeteer = require('puppeteer-extra')
const StealthPlugin = require('puppeteer-extra-plugin-stealth')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')

puppeteer.use(StealthPlugin())

// Helper function to extract image URL from Macy's file path
function getImageUrl(
  filePath,
  baseUrl = 'https://slimages.macysassets.com/is/image/MCY/products/'
) {
  if (!filePath) return ''
  return baseUrl + filePath.replace(/\.(tif|jpg|jpeg|png)$/i, '')
}

// Helper function to calculate discount percentage
function calculateDiscount(originalPrice, finalPrice) {
  if (!originalPrice || !finalPrice || originalPrice <= finalPrice) return 0
  return Math.round(((originalPrice - finalPrice) / originalPrice) * 100)
}

// Helper function to determine gender from product name and category
function determineGender(name, typeName = '') {
  const lowerName = (name + ' ' + typeName).toLowerCase()
  if (lowerName.includes("men's") || lowerName.includes('mens')) return 'Men'
  if (lowerName.includes("women's") || lowerName.includes('womens'))
    return 'Women'
  if (lowerName.includes("boy's") || lowerName.includes('boys')) return 'Boys'
  if (lowerName.includes("girl's") || lowerName.includes('girls'))
    return 'Girls'
  if (
    lowerName.includes("kid's") ||
    lowerName.includes('kids') ||
    lowerName.includes('children')
  )
    return 'Kids'
  return 'Unisex'
}

// Helper function to determine category
function determineCategory(typeName, name) {
  if (!typeName) return 'Unknown'

  const typeMapping = {
    'T-SHIRT': 'T-Shirts',
    SWEATSHIRT: 'Hoodies & Sweatshirts',
    HOODIE: 'Hoodies & Sweatshirts',
    JEANS: 'Jeans',
    PANTS: 'Pants',
    SHORTS: 'Shorts',
    DRESS: 'Dresses',
    BLOUSE: 'Blouses',
    SHIRT: 'Shirts',
    JACKET: 'Jackets',
    COAT: 'Coats',
    SWEATER: 'Sweaters',
    CARDIGAN: 'Cardigans',
    SKIRT: 'Skirts',
    SUIT: 'Suits',
    BLAZER: 'Blazers',
    SWIMWEAR: 'Swimwear',
    UNDERWEAR: 'Underwear',
    SLEEPWEAR: 'Sleepwear',
    ACTIVEWEAR: 'Activewear',
    SHOES: 'Shoes',
    SNEAKERS: 'Sneakers',
    BOOTS: 'Boots',
    SANDALS: 'Sandals',
    ACCESSORIES: 'Accessories',
    HANDBAG: 'Handbags',
    WALLET: 'Wallets',
    JEWELRY: 'Jewelry',
    WATCH: 'Watches',
  }

  return typeMapping[typeName.toUpperCase()] || typeName
}

// Function to process Macy's product data similar to shopify format
function processMacysProducts(rawProducts) {
  const formattedProducts = []

  for (const item of rawProducts) {
    const product = item.product
    if (!product || !product.id) continue

    console.log(`Processing product: ${product.detail?.name || 'Unknown'}`)

    // Extract basic product information
    const productId = product.id.toString()
    const name = product.detail?.name || 'Unknown Product'
    const description =
      product.detail?.secondaryDescription || 'Unknown Product'
    const brand = product.detail?.brand || 'Unknown Brand'
    const typeName = product.detail?.typeName || ''
    const category = determineCategory(typeName, name)
    const gender = determineGender(name, typeName)

    // Extract review data
    const reviewStats = product.detail?.reviewStatistics?.aggregate || {}
    const averageRating = reviewStats.rating || 0
    const reviewCount = reviewStats.count || 0

    // Extract pricing information
    const pricing = product.pricing?.price
    let originalPrice = 0
    let sellingPrice = 0
    let salePrice = null
    let isOnSale = false
    let discount = 0

    if (pricing?.tieredPrice) {
      // Find regular price
      const regularPriceItem = pricing.tieredPrice.find(
        (p) => p.label === '[PRICE]' && p.values?.[0]?.type === 'regular'
      )
      if (regularPriceItem) {
        originalPrice = regularPriceItem.values[0].value || 0
        sellingPrice = originalPrice
      }

      // Find discount price
      const discountPriceItem = pricing.tieredPrice.find(
        (p) => p.label.includes('Now') && p.values?.[0]?.type === 'discount'
      )
      if (discountPriceItem) {
        salePrice = discountPriceItem.values[0].value || 0
        sellingPrice = salePrice
        isOnSale = true
      }

      // Check if marked as on sale
      if (pricing.priceType?.onSale) {
        isOnSale = true
      }
    }

    const finalPrice = sellingPrice
    discount = calculateDiscount(originalPrice, finalPrice)

    // Extract availability
    const isInStock = product.availability?.available || false
    const isActive = product.availability?.active || false

    // Build the base product structure
    const formattedProduct = {
      parent_product_id: productId,
      name: name,
      description, // Macy's doesn't provide description in this API
      category: category,
      retailer_domain: 'macys.com',
      brand: brand,
      gender: gender,
      materials: '', // Not available in this API response
      return_policy_link:
        'https://customerservice-macys.com/articles/what-is-macys-return-policy',
      return_policy: '',
      size_chart: '',
      available_bank_offers: '',
      available_coupons: '',
      variants: [],
      operation_type: 'INSERT',
      source: 'macys',
    }

    // Extract colors and create variants
    const colors = product.traits?.colors
    if (colors?.colorMap && colors.colorMap.length > 0) {
      // Create a variant for each color
      for (const color of colors.colorMap) {
        const variant = createVariant(product, color, {
          originalPrice,
          sellingPrice,
          salePrice,
          finalPrice,
          discount,
          isOnSale,
          isInStock,
          averageRating,
          reviewCount,
        })
        formattedProduct.variants.push(variant)
      }
    } else {
      // Create a single variant if no colors
      const variant = createVariant(product, null, {
        originalPrice,
        sellingPrice,
        salePrice,
        finalPrice,
        discount,
        isOnSale,
        isInStock,
        averageRating,
        reviewCount,
      })
      formattedProduct.variants.push(variant)
    }

    formattedProducts.push(formattedProduct)
  }

  return formattedProducts
}

// Function to create individual variant
function createVariant(product, color, priceData) {
  const productUrl = `https://www.macys.com${
    product.identifier?.productUrl || ''
  }`

  // Get images - prefer color-specific images if available
  let imageUrl = ''
  let alternateImages = []

  if (color?.imagery) {
    // Use color-specific images
    imageUrl = getImageUrl(color.imagery.primaryImage?.filePath)
    if (color.imagery.additionalImageSource) {
      alternateImages = color.imagery.additionalImageSource
        .map((img) => getImageUrl(img.filePath))
        .filter((url) => url && url !== imageUrl)
    }
  } else {
    // Use product-level images
    imageUrl = getImageUrl(product.imagery?.primaryImage?.filePath)
    if (product.imagery?.additionalImageSource) {
      alternateImages = product.imagery.additionalImageSource
        .map((img) => getImageUrl(img.filePath))
        .filter((url) => url && url !== imageUrl)
    }
  }

  // Extract color information
  const colorName = color?.name || color?.normalName || ''
  const colorId = color?.id?.toString() || ''

  return {
    price_currency: 'USD',
    original_price: priceData.originalPrice,
    link_url: productUrl,
    deeplink_url: productUrl,
    image_url: imageUrl,
    alternate_image_urls: alternateImages,
    is_on_sale: priceData.isOnSale,
    is_in_stock: priceData.isInStock,
    size: '', // Size information not available in this API response
    color: colorName,
    mpn: uuidv5(
      colorName || 'NO_COLOR',
      '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
    ),
    ratings_count: priceData.reviewCount,
    average_ratings: priceData.averageRating,
    review_count: priceData.reviewCount,
    selling_price: priceData.sellingPrice,
    sale_price: priceData.salePrice,
    final_price: priceData.finalPrice,
    discount: priceData.discount,
    operation_type: 'INSERT',
    variant_id: colorId || uuidv4(),
  }
}

// Function to save processed data to files
function saveProcessedData(formattedProducts, outputDir = 'output') {
  // Create directory structure
  const dirPath = path.join(__dirname, outputDir, 'US', 'macys-US')

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }

  // Save formatted data as JSON
  const jsonFilePath = path.join(dirPath, 'catalog.json')
  const catalogData = {
    store_info: {
      name: "Macy's",
      domain: 'macys.com',
      currency: 'USD',
      country: 'US',
      total_products: formattedProducts.length,
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

  return {
    jsonPath: gzippedFilePath,
    jsonlPath: jsonlFilePath,
    totalProducts: formattedProducts.length,
  }
}

async function scrapeApiData(url) {
  const browser = await puppeteer.launch({
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu',
    ],
  })
  const page = await browser.newPage()

  // Set a realistic user agent
  await page.setUserAgent(
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  )

  // Set up request interception to capture the API response
  await page.setRequestInterception(true)

  let apiData = null

  page.on('request', (request) => {
    request.continue()
  })

  page.on('response', async (response) => {
    const responseUrl = response.url()
    if (responseUrl.includes('/xapi/discover/v1/page') && !apiData) {
      try {
        const responseBody = await response.text()
        console.log('API response captured')
        const fullApiData = JSON.parse(responseBody)

        // Extract the specific collection data
        try {
          apiData =
            fullApiData?.body?.canvas?.rows?.[0]?.rowSortableGrid?.zones?.[1]
              ?.sortableGrid?.collection
          if (!apiData) {
            console.warn(
              'Could not find collection data at expected path, returning full response'
            )
            apiData = fullApiData
          }
        } catch (extractError) {
          console.warn(
            'Error extracting collection data, returning full response:',
            extractError
          )
          apiData = fullApiData
        }
      } catch (error) {
        console.error('Error capturing API response:', error)
      }
    }
  })

  console.log('Navigating to page...')
  await page.goto(url, { waitUntil: 'networkidle2' })

  // Wait for the API calls to complete
  await new Promise((resolve) => setTimeout(resolve, 5000))

  await browser.close()

  if (!apiData) {
    throw new Error(
      'No API response was captured. The page may not have made the expected API call.'
    )
  }

  return apiData
}

// Main function to scrape and process Macy's data
async function scrapeMacysData(url) {
  try {
    console.log("Starting Macy's data scraping...")

    // Scrape the raw data
    const rawData = await scrapeApiData(url)
    console.log(`Raw data captured, found ${rawData.length} products`)

    // Process the data similar to shopify format
    const processedProducts = processMacysProducts(rawData)
    console.log(`Processed ${processedProducts.length} products`)

    // Save the processed data
    const result = saveProcessedData(processedProducts)

    console.log("✅ Macy's data processing completed successfully!")
    console.log(`📊 Summary:`)
    console.log(`  Total products: ${result.totalProducts}`)
    console.log(`  JSON file: ${result.jsonPath}`)
    console.log(`  JSONL file: ${result.jsonlPath}`)

    return result
  } catch (error) {
    console.error("❌ Error processing Macy's data:", error.message)
    throw error
  }
}

// Export the functions for use in other modules
module.exports = {
  scrapeApiData,
  scrapeMacysData,
  processMacysProducts,
  saveProcessedData,
}

// Example usage (uncomment to test):
;(async () => {
  try {
    const result = await scrapeMacysData(
      'https://www.macys.com/xapi/discover/v1/page?pathname=/shop/mens-clothing/all-mens-clothing&id=197651&_navigationType=BROWSE&_shoppingMode=SITE&sortBy=ORIGINAL&productsPerPage=60&pageIndex=2&_application=SITE&_regionCode=US&currencyCode=USD&size=medium&spItemsVersion=1.1&utagId=0197693dd0ef0020b8aa4749798005075001606d01328&_deviceType=DESKTOP&_customerState=GUEST'
    )
    console.log('Scraping completed successfully:', result)
  } catch (error) {
    console.error('Error:', error.message)
  }
})()
