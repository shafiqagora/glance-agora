// Update Variant Images Script
// Updates variant images in processed.json using data fetched from Gap API
// Writes the updated data to a new file: processed-with-images.json

const fs = require('fs')
const path = require('path')
const axios = require('axios')
const { filterValidProducts } = require('./validate-catalog')
const zlib = require('zlib')

// Configuration
const GAP_API_BASE_URL = 'https://api.gap.com/commerce/search/products/v2/cc'
const OUTPUT_DIR = path.join(__dirname, 'output', 'US', 'gap-US')
const PROCESSED_FILE = path.join(OUTPUT_DIR, 'processed.json')
const OUTPUT_FILE = path.join(OUTPUT_DIR, 'processed-with-images.json')
const CATALOG_JSON_FILE = path.join(OUTPUT_DIR, 'catalog.json')
const CATALOG_JSONL_FILE = path.join(OUTPUT_DIR, 'catalog.jsonl')
const CATALOG_GZIP_FILE = path.join(OUTPUT_DIR, 'catalog.jsonl.gz')

/**
 * Fetch products from Gap API for specific categories
 */
async function fetchProductsFromGapAPI() {
  console.log('\n🌐 Fetching products from Gap API...')

  // Define categories to fetch (same as gap-crawler)
  const categories = [
    {
      name: 'Women Shop All',
      gender: 'Women',
      cid: '1127938',
    },
    {
      name: 'Women Dresses',
      gender: 'Women',
      cid: '1127942',
    },
    {
      name: 'Women Tops',
      gender: 'Women',
      cid: '1127943',
    },
    {
      name: 'Women Jeans',
      gender: 'Women',
      cid: '1127944',
    },
    {
      name: 'Men Jeans',
      gender: 'Men',
      cid: '1127945',
    },
    {
      name: 'Men T-Shirts',
      gender: 'Men',
      cid: '1127946',
    },
  ]

  let allProducts = []
  const targetProductsPerCategory = 500 // Adjust as needed

  for (const category of categories) {
    console.log(`\n📦 Fetching ${category.name}...`)

    let currentPage = 0
    let categoryProducts = []
    let isLastPage = false

    while (!isLastPage && categoryProducts.length < targetProductsPerCategory) {
      const apiUrl = `${GAP_API_BASE_URL}?pageSize=50&pageNumber=${currentPage}&ignoreInventory=false&cid=${category.cid}&vendor=constructorio&includeMarketingFlagsDetails=true&brand=gap&locale=en_US&market=us`

      try {
        console.log(`  Fetching page ${currentPage}...`)

        const response = await axios.get(apiUrl, {
          headers: {
            'User-Agent':
              'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            Accept: 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
          },
          timeout: 30000,
        })

        const pageData = response.data

        // Check pagination
        if (pageData.pagination) {
          const totalPages = parseInt(pageData.pagination.pageNumberTotal) || 0
          const currentPageNum = parseInt(pageData.pagination.currentPage) || 0
          isLastPage = currentPageNum >= totalPages - 1
          console.log(
            `  Page ${currentPage}: ${currentPageNum + 1}/${totalPages} (${
              pageData.products?.length || 0
            } products)`
          )
        }

        // Add products from this page
        if (pageData.products && pageData.products.length > 0) {
          categoryProducts = categoryProducts.concat(pageData.products)
          console.log(
            `  Total for ${category.name}: ${categoryProducts.length} products`
          )
        } else {
          console.log(`  No products found on page ${currentPage}`)
          isLastPage = true
        }

        currentPage++

        // Add delay between requests to be respectful
        await new Promise((resolve) => setTimeout(resolve, 1000))
      } catch (error) {
        console.error(
          `  ❌ Error fetching page ${currentPage}: ${error.message}`
        )
        isLastPage = true
      }
    }

    // Limit to target count
    if (categoryProducts.length > targetProductsPerCategory) {
      categoryProducts = categoryProducts.slice(0, targetProductsPerCategory)
    }

    console.log(
      `  ✅ ${category.name}: ${categoryProducts.length} products fetched`
    )

    allProducts = allProducts.concat(categoryProducts)
  }

  console.log(`\n✅ Total products fetched from API: ${allProducts.length}`)

  return {
    products: allProducts,
    store_info: {
      name: 'Gap',
      domain: 'gap.com',
      currency: 'USD',
      country: 'US',
      total_products: allProducts.length,
      categories: ['Women', 'Men', 'Kids', 'Baby'],
      crawled_at: new Date().toISOString(),
    },
  }
}

/**
 * Extract color name from variant color field
 * Handles cases like "White Fresh white" -> "Fresh white"
 */
function normalizeColorName(colorName) {
  if (!colorName) return ''
  return colorName.trim().toLowerCase()
}

/**
 * Find matching color data from products API data
 */
function findMatchingColor(productId, colorName, productsData) {
  // Find the product in products.json
  const product = productsData.products.find(
    (p) => p.styleId?.toString() === productId?.toString()
  )

  if (!product || !product.styleColors) {
    return null
  }

  const normalizedSearchColor = normalizeColorName(colorName)

  // Try to find exact match first
  let matchedColor = product.styleColors.find((color) => {
    const ccShortDesc = normalizeColorName(color.ccShortDescription || '')
    const ccName = normalizeColorName(color.ccName || '')
    const fullName = normalizeColorName(
      `${color.ccName} ${color.ccShortDescription}`
    )

    return (
      ccShortDesc === normalizedSearchColor ||
      ccName === normalizedSearchColor ||
      fullName === normalizedSearchColor
    )
  })

  return matchedColor
}

/**
 * Extract and format images from color data
 */
function extractImagesFromColor(colorData) {
  if (!colorData || !colorData.images || colorData.images.length === 0) {
    return {
      imageUrl: '',
      alternateImages: [],
    }
  }

  // Preferred image types for main image (in order of preference)
  const mainImageTypes = ['VLI', 'OVI1', 'P01', 'VI', 'AV1']

  // Find the best main image
  let mainImage = null
  for (const type of mainImageTypes) {
    mainImage = colorData.images.find((img) => img.type === type)
    if (mainImage) break
  }

  // Fallback to first image if no preferred type found
  if (!mainImage) {
    mainImage = colorData.images[0]
  }

  const imageUrl =
    mainImage && mainImage.path
      ? mainImage.path.startsWith('http')
        ? mainImage.path
        : `https://www.gap.com${mainImage.path.startsWith('/') ? '' : '/'}${
            mainImage.path
          }`
      : ''

  // Get alternate images (exclude the main image)
  const alternateImages = colorData.images
    .filter((img) => img !== mainImage && img.path)
    .map((img) => {
      const imgPath = img.path
      return imgPath.startsWith('http')
        ? imgPath
        : `https://www.gap.com${imgPath.startsWith('/') ? '' : '/'}${imgPath}`
    })
    .filter((url) => url) // Remove empty URLs

  return {
    imageUrl,
    alternateImages,
  }
}

/**
 * Update variant images in processed data
 */
function updateVariantImages(processedData, productsData) {
  let totalVariants = 0
  let updatedVariants = 0
  let skippedVariants = 0

  console.log('\n🔄 Updating variant images...')

  for (const product of processedData.products) {
    const productId = product.parent_product_id

    for (const variant of product.variants) {
      totalVariants++

      // Find matching color in products data
      const matchedColor = findMatchingColor(
        productId,
        variant.color,
        productsData
      )

      if (matchedColor) {
        const { imageUrl, alternateImages } =
          extractImagesFromColor(matchedColor)

        if (imageUrl) {
          variant.image_url = imageUrl
          variant.alternate_image_urls = alternateImages
          updatedVariants++

          if (totalVariants % 500 === 0) {
            console.log(
              `  ✅ Updated ${updatedVariants}/${totalVariants} variants...`
            )
          }
        } else {
          skippedVariants++
          console.log(
            `  ⚠️ No images found for variant: ${product.name} - ${variant.color}`
          )
        }
      } else {
        skippedVariants++
        console.log(
          `  ⚠️ No matching color found for variant: ${product.name} - ${variant.color}`
        )
      }
    }
  }

  console.log(`\n📊 Image Update Results:`)
  console.log(`  Total variants: ${totalVariants}`)
  console.log(`  Updated variants: ${updatedVariants}`)
  console.log(`  Skipped variants: ${skippedVariants}`)

  return {
    totalVariants,
    updatedVariants,
    skippedVariants,
  }
}

/**
 * Generate catalog files from updated data
 */
function generateCatalogFiles(updatedData, storeData) {
  console.log('\n📚 Generating catalog files...')

  // Validate products
  const validationResult = filterValidProducts(updatedData.products)

  console.log(`\n📊 Validation Results:`)
  console.log(`  Total products: ${validationResult.totalCount}`)
  console.log(`  Valid products: ${validationResult.validCount}`)
  console.log(`  Invalid products: ${validationResult.invalidCount}`)
  console.log(
    `  Variants filtered: ${validationResult.totalVariantsFiltered || 0}`
  )

  const formattedProducts = validationResult.validProducts

  if (formattedProducts.length === 0) {
    console.log('⚠️ No valid products after validation')
    return null
  }

  // Create catalog data structure
  const catalogData = {
    store_info: {
      name: storeData.name || 'Gap',
      domain: 'gap.com',
      currency: storeData.currency || 'USD',
      country: storeData.country || 'US',
      total_products: formattedProducts.length,
      categories: ['Women', 'Men', 'Kids', 'Baby'],
      crawled_at: storeData.crawled_at || new Date().toISOString(),
      validation: {
        total_processed: validationResult.totalCount,
        valid_products: validationResult.validCount,
        invalid_products: validationResult.invalidCount,
        variants_filtered: validationResult.totalVariantsFiltered || 0,
      },
    },
    products: formattedProducts,
  }

  // Save catalog.json
  fs.writeFileSync(
    CATALOG_JSON_FILE,
    JSON.stringify(catalogData, null, 2),
    'utf8'
  )
  console.log(`✅ JSON file generated: ${CATALOG_JSON_FILE}`)

  // Create JSONL file
  const jsonlContent = formattedProducts
    .map((product) => JSON.stringify(product))
    .join('\n')
  fs.writeFileSync(CATALOG_JSONL_FILE, jsonlContent, 'utf8')
  console.log(`✅ JSONL file generated: ${CATALOG_JSONL_FILE}`)

  // Gzip the JSONL file
  const jsonlBuffer = fs.readFileSync(CATALOG_JSONL_FILE)
  const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
  fs.writeFileSync(CATALOG_GZIP_FILE, gzippedBuffer)
  console.log(`✅ Gzipped JSONL file generated: ${CATALOG_GZIP_FILE}`)

  return {
    jsonPath: CATALOG_GZIP_FILE,
    totalProducts: formattedProducts.length,
    validationResult: validationResult,
  }
}

/**
 * Main function
 */
async function main() {
  try {
    console.log('🚀 Starting variant image update process...')
    console.log(`📂 Output directory: ${OUTPUT_DIR}`)

    // Check if processed file exists
    if (!fs.existsSync(PROCESSED_FILE)) {
      console.error(`❌ Error: ${PROCESSED_FILE} not found`)
      process.exit(1)
    }

    // Load processed data
    console.log('\n📖 Loading processed.json...')
    const processedData = JSON.parse(fs.readFileSync(PROCESSED_FILE, 'utf8'))
    console.log(`  Found ${processedData.products.length} products`)

    // Fetch fresh products data from Gap API
    const productsData = await fetchProductsFromGapAPI()

    // Update variant images
    const updateStats = updateVariantImages(processedData, productsData)

    // Save updated data to new file
    console.log(`\n💾 Saving updated data to: ${OUTPUT_FILE}`)
    fs.writeFileSync(
      OUTPUT_FILE,
      JSON.stringify(processedData, null, 2),
      'utf8'
    )
    console.log('✅ Updated data saved successfully!')

    // Generate catalog files
    const catalogResult = generateCatalogFiles(
      processedData,
      productsData.store_info
    )

    if (catalogResult) {
      console.log(`\n🎉 SUCCESS! All files generated:`)
      console.log(`  📄 ${OUTPUT_FILE}`)
      console.log(`  📄 ${CATALOG_JSON_FILE}`)
      console.log(`  📄 ${CATALOG_JSONL_FILE}`)
      console.log(`  📦 ${CATALOG_GZIP_FILE}`)
      console.log(`\n📊 Final Statistics:`)
      console.log(`  Total variants processed: ${updateStats.totalVariants}`)
      console.log(
        `  Variants with updated images: ${updateStats.updatedVariants}`
      )
      console.log(`  Variants skipped: ${updateStats.skippedVariants}`)
      console.log(`  Final catalog products: ${catalogResult.totalProducts}`)
    }

    return true
  } catch (error) {
    console.error('❌ Error during image update:', error)
    console.error(error.stack)
    process.exit(1)
  }
}

// Run the script
if (require.main === module) {
  main()
    .then((result) => {
      if (result) {
        console.log('\n✅ Script completed successfully')
        process.exit(0)
      } else {
        console.log('\n❌ Script failed')
        process.exit(1)
      }
    })
    .catch((error) => {
      console.error('❌ Script failed:', error)
      process.exit(1)
    })
}

module.exports = { main }
