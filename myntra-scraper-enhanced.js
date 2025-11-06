const axios = require('axios')
const fs = require('fs').promises
const path = require('path')
const zlib = require('zlib')
const { v4: uuidv4, v5: uuidv5 } = require('uuid')
const sanitizeHtml = require('sanitize-html')
const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const {
  retryRequestWithProxyRotation,
  createAxiosInstance,
} = require('./utils/helper')

class MyntraScraperEnhanced {
  constructor() {
    this.baseUrl = 'https://www.myntra.com'
    this.outputFile = 'myntra-all-categories.json'
    this.cookies = ''
    this.currentProxy = null // Store current proxy for session binding
    this.currentCategory = null // Store current category for session refresh
    this.rowsPerPage = 50
    this.productsPerCategory = 500 // Limit products per category
    this.processedProductIds = new Set() // Track processed product IDs to avoid duplicates
    this.categories = this.getCategories()
    this.requestCount = 0 // Track number of requests made
    this.maxRequestsPerSession = 5 // Reset session after this many requests
  }

  getCategories() {
    return [
      { name: 'women-dresses', url: 'women-dresses', apiPath: 'women-dresses' },
      { name: 'women-tops', url: 'women-tops', apiPath: 'women-tops' },
      {
        name: 'women-kurtas-kurtis',
        url: 'women-kurtas-kurtis',
        apiPath: 'women-kurtas-kurtis',
      },
      { name: 'women-sarees', url: 'women-sarees', apiPath: 'women-sarees' },
      { name: 'women-jeans', url: 'women-jeans', apiPath: 'women-jeans' },
      {
        name: 'women-trousers',
        url: 'women-trousers',
        apiPath: 'women-trousers',
      },
      { name: 'women-skirts', url: 'women-skirts', apiPath: 'women-skirts' },
      {
        name: 'women-shorts-capris',
        url: 'women-shorts-capris',
        apiPath: 'women-shorts-capris',
      },
      {
        name: 'women-ethnic-wear',
        url: 'women-ethnic-wear',
        apiPath: 'women-ethnic-wear',
      },
      { name: 'women-jackets', url: 'women-jackets', apiPath: 'women-jackets' },
      {
        name: 'women-sweaters',
        url: 'women-sweaters',
        apiPath: 'women-sweaters',
      },
      { name: 'women-blazers', url: 'women-blazers', apiPath: 'women-blazers' },
      {
        name: 'women-jumpsuits',
        url: 'women-jumpsuits',
        apiPath: 'women-jumpsuits',
      },
      {
        name: 'women-leggings',
        url: 'women-leggings',
        apiPath: 'women-leggings',
      },
      {
        name: 'women-dupattas',
        url: 'women-dupattas',
        apiPath: 'women-dupattas',
      },
      {
        name: 'women-salwar-suits',
        url: 'women-salwar-suits',
        apiPath: 'women-salwar-suits',
      },
      { name: 'men-shirts', url: 'men-shirts', apiPath: 'men-shirts' },
      { name: 'men-tshirts', url: 'men-tshirts', apiPath: 'men-tshirts' },
      { name: 'men-jeans', url: 'men-jeans', apiPath: 'men-jeans' },
      { name: 'men-trousers', url: 'men-trousers', apiPath: 'men-trousers' },
      { name: 'men-shorts', url: 'men-shorts', apiPath: 'men-shorts' },
      {
        name: 'men-ethnic-wear',
        url: 'men-ethnic-wear',
        apiPath: 'men-ethnic-wear',
      },
      { name: 'men-jackets', url: 'men-jackets', apiPath: 'men-jackets' },
      { name: 'men-sweaters', url: 'men-sweaters', apiPath: 'men-sweaters' },
      { name: 'men-blazers', url: 'men-blazers', apiPath: 'men-blazers' },
      { name: 'men-suits', url: 'men-suits', apiPath: 'men-suits' },
      {
        name: 'men-trackpants',
        url: 'men-trackpants',
        apiPath: 'men-trackpants',
      },
      {
        name: 'men-kurta-sets',
        url: 'men-kurta-sets',
        apiPath: 'men-kurta-sets',
      },
      {
        name: 'men-waistcoats',
        url: 'men-waistcoats',
        apiPath: 'men-waistcoats',
      },
    ]
  }

  async establishSession(category) {
    try {
      console.log(`Establishing session by visiting ${category.name} page...`)

      const response = await retryRequestWithProxyRotation(
        async (axiosInstance, currentProxy) => {
          // Store the proxy used for this session
          this.currentProxy = currentProxy
          console.log(
            `Session will be bound to proxy: ${
              currentProxy ? 'PROXY_ENABLED' : 'NO_PROXY'
            }`
          )

          return await axiosInstance.get(`${this.baseUrl}/${category.url}`, {
            headers: {
              'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              Accept:
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
              'Accept-Language': 'en-US,en;q=0.9',
              'Accept-Encoding': 'gzip, deflate, br',
              Connection: 'keep-alive',
              'Upgrade-Insecure-Requests': '1',
              'Sec-Fetch-Dest': 'document',
              'Sec-Fetch-Mode': 'navigate',
              'Sec-Fetch-Site': 'none',
              'sec-ch-ua':
                '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
              'sec-ch-ua-mobile': '?0',
              'sec-ch-ua-platform': '"macOS"',
            },
          })
        }
      )

      if (response.headers['set-cookie']) {
        this.cookies = response.headers['set-cookie'].join('; ')
        console.log(
          `Session established successfully for ${category.name} with cookies`
        )
      }

      return response.status === 200
    } catch (error) {
      console.error(
        `Error establishing session for ${category.name}:`,
        error.message
      )
      return false
    }
  }

  // Check if session needs refreshing based on request count
  shouldRefreshSession() {
    return this.requestCount >= this.maxRequestsPerSession
  }

  // Reset session and request counter
  async refreshSession(category) {
    console.log(`🔄 Refreshing session after ${this.requestCount} requests...`)
    this.cookies = ''
    this.requestCount = 0

    // Wait a bit before establishing new session to avoid rate limiting
    await new Promise((resolve) => setTimeout(resolve, 2000))

    return await this.establishSession(category)
  }

  async fetchData(category, page = 1) {
    try {
      // Check if session needs refreshing
      if (this.shouldRefreshSession()) {
        await this.refreshSession(category)
      }

      // Increment request counter
      this.requestCount++

      // Calculate offset: page 1 = 0, page 2 = 49, page 3 = 99, etc.
      const offset = page === 1 ? 0 : (page - 1) * this.rowsPerPage - 1
      const apiUrl = `https://www.myntra.com/gateway/v2/search/${category.apiPath}?rows=${this.rowsPerPage}&o=${offset}`

      console.log(
        `Fetching data from Myntra API for ${category.name} page ${page}... (Request #${this.requestCount})`
      )
      console.log(`URL: ${apiUrl}`)
      console.log(`Offset: ${offset}, Rows: ${this.rowsPerPage}`)

      const headers = {
        'User-Agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        Accept: 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        Connection: 'keep-alive',
        Referer: `https://www.myntra.com/${category.url}`,
        Origin: 'https://www.myntra.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'sec-ch-ua':
          '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'X-Requested-With': 'XMLHttpRequest',
      }

      if (this.cookies) {
        headers['Cookie'] = this.cookies
      }

      // Use longer timeout for subsequent pages (they tend to be slower)
      const timeout = page > 1 ? 90000 : 60000 // 90s for page 2+, 60s for page 1

      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          // Override the default timeout for this specific request
          axiosInstance.defaults.timeout = timeout
          return await axiosInstance.get(apiUrl, { headers })
        }
      )

      console.log(`Response status: ${response.status}`)

      if (response.status === 200) {
        console.log(
          `Successfully fetched data from Myntra API for ${category.name} page ${page}`
        )
        return response.data
      } else {
        console.log(
          `API returned status ${response.status} for ${category.name} page ${page}`
        )
        return {
          error: true,
          status: response.status,
          message: 'API returned non-200 status',
          data: response.data,
        }
      }
    } catch (error) {
      console.error(
        `Error fetching data from Myntra API for ${category.name} page ${page}:`,
        error.message
      )

      if (error.response) {
        console.error('Response status:', error.response.status)
        return {
          error: true,
          status: error.response.status,
          message: error.message,
          data: error.response.data,
        }
      } else {
        return {
          error: true,
          message: error.message,
          details: 'Network or request error',
        }
      }
    }
  }

  async fetchProductColorVariants(productId) {
    try {
      // Check if session needs refreshing
      if (this.shouldRefreshSession() && this.currentCategory) {
        await this.refreshSession(this.currentCategory)
      }

      // Increment request counter for product detail requests too
      this.requestCount++

      console.log(
        `Fetching color variants for product ${productId}... (Request #${this.requestCount})`
      )

      const apiUrl = `https://www.myntra.com/gateway/v2/product/${productId}/related?colors=true`

      const headers = {
        'User-Agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        Accept: 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        Connection: 'keep-alive',
        Referer: `https://www.myntra.com/product/${productId}`,
        Origin: 'https://www.myntra.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'sec-ch-ua':
          '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'X-Requested-With': 'XMLHttpRequest',
      }

      if (this.cookies) {
        headers['Cookie'] = this.cookies
      }

      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          return await axiosInstance.get(apiUrl, { headers })
        }
      )

      if (response.status === 200 && response.data?.related) {
        // Find ColourVariants in the related array
        const colourVariants = response.data.related.find(
          (item) => item.type === 'ColourVariants'
        )

        if (colourVariants && colourVariants.products) {
          console.log(
            `Found ${colourVariants.products.length} color variants for product ${productId}`
          )
          return colourVariants.products
        }
      }

      console.log(`No color variants found for product ${productId}`)
      return []
    } catch (error) {
      console.error(
        `Error fetching color variants for product ${productId}:`,
        error.message
      )
      return []
    } finally {
    }
  }

  async fetchProductDetails(productId) {
    try {
      // Check if session needs refreshing
      if (this.shouldRefreshSession() && this.currentCategory) {
        await this.refreshSession(this.currentCategory)
      }

      // Increment request counter for product detail requests too
      this.requestCount++

      console.log(
        `Fetching details for product ${productId}... (Request #${this.requestCount})`
      )

      const apiUrl = `https://www.myntra.com/gateway/v2/product/${productId}`

      const headers = {
        'User-Agent':
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        Accept: 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        Connection: 'keep-alive',
        Referer: `https://www.myntra.com/product/${productId}`,
        Origin: 'https://www.myntra.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'sec-ch-ua':
          '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'X-Requested-With': 'XMLHttpRequest',
      }

      if (this.cookies) {
        headers['Cookie'] = this.cookies
      }

      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          return await axiosInstance.get(apiUrl, { headers })
        }
      )

      if (response.status === 200 && response.data?.style) {
        // Find ColourVariants in the related array
        return response.data.style
      }

      console.log(`No color variants found for product ${productId}`)
      return {}
    } catch (error) {
      console.error(
        `Error fetching color variants for product ${productId}:`,
        error.message
      )
      return []
    } finally {
    }
  }

  async tryAlternativeApproach(category) {
    try {
      console.log(
        `Trying alternative approach for ${
          category?.name || 'unknown category'
        } - fetching HTML page and extracting data...`
      )

      const categoryUrl = category ? category.url : 'men-topwear'
      const response = await retryRequestWithProxyRotation(
        async (axiosInstance) => {
          return await axiosInstance.get(`${this.baseUrl}/${categoryUrl}`, {
            headers: {
              'User-Agent':
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
              Accept:
                'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
              'Accept-Language': 'en-US,en;q=0.9',
              Connection: 'keep-alive',
              'Upgrade-Insecure-Requests': '1',
            },
          })
        }
      )

      if (response.status === 200) {
        console.log('Successfully fetched HTML page')

        // Look for JSON data in script tags
        const html = response.data
        const scriptMatches = html.match(
          /<script[^>]*>[\s\S]*?window\.__myx\s*=\s*({[\s\S]*?});[\s\S]*?<\/script>/g
        )

        if (scriptMatches) {
          for (const match of scriptMatches) {
            const jsonMatch = match.match(/window\.__myx\s*=\s*({[\s\S]*?});/)
            if (jsonMatch) {
              try {
                const data = JSON.parse(jsonMatch[1])
                console.log('Found embedded JSON data in HTML')
                return data
              } catch (parseError) {
                console.log('Could not parse embedded JSON')
              }
            }
          }
        }

        return {
          error: false,
          message: 'HTML page fetched but no structured data found',
          htmlLength: html.length,
        }
      }

      return {
        error: true,
        status: response.status,
        message: 'Failed to fetch HTML page',
      }
    } catch (error) {
      console.error('Error with alternative approach:', error.message)
      return {
        error: true,
        message: error.message,
      }
    }
  }

  // Utility functions similar to shopify-crawler-csv.js
  calculateDiscount(originalPrice, finalPrice) {
    if (!originalPrice || !finalPrice || originalPrice <= finalPrice) return 0
    return Math.round(((originalPrice - finalPrice) / originalPrice) * 100)
  }

  extractSizesFromString(sizesString) {
    if (!sizesString) return ['']
    return sizesString
      .split(',')
      .map((size) => size.trim())
      .filter((size) => size)
  }

  // Save product to MongoDB
  async saveProductToMongoDB(productData) {
    try {
      // Check if product already exists
      const existingProduct = await Product.findOne({
        parent_product_id: productData.parent_product_id,
      })

      if (existingProduct) {
        console.log(
          `Product ${productData.name} already exists in database, skipping...`
        )
        return { operation: 'SKIPPED', product: existingProduct }
      }

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
      throw error
    }
  }

  async formatMyntraProducts(rawData, categoryName) {
    if (!rawData || !rawData.products || rawData.products.length === 0) {
      console.log('No products found in raw data')
      return []
    }

    const formattedProducts = []
    const mongoResults = {
      inserted: 0,
      skipped: 0,
      errors: 0,
      duplicatesSkipped: 0,
    }

    for (const product of rawData.products) {
      try {
        // Check for duplicates using product ID
        if (this.processedProductIds.has(product.productId.toString())) {
          console.log(
            `⚠️  Skipping duplicate product: ${product.productName} (ID: ${product.productId})`
          )
          mongoResults.duplicatesSkipped++
          continue
        }

        // Add to processed set
        this.processedProductIds.add(product.productId.toString())

        console.log(
          `Processing product: ${product.productName} from ${categoryName}`
        )

        // Extract sizes from the comma-separated sizes field
        const sizes = this.extractSizesFromString(product.sizes)

        // Fetch color variants for this product
        const colorVariants = await this.fetchProductColorVariants(
          product.productId
        )
        let productDetails = await this.fetchProductDetails(product.productId)

        // Get discount information for the main product
        const originalPrice = product.mrp // Convert from paise to rupees
        const sellingPrice = product.price
        const discount = this.calculateDiscount(originalPrice, sellingPrice)
        const isOnSale = discount > 0

        // Get images
        const images = product.images || []
        const primaryImage =
          product.searchImage || (images.length > 0 ? images[0].src : '')
        const alternateImages = images
          .filter((img) => img.src && img.src !== primaryImage)
          .map((img) => img.src)
        const description =
          productDetails.productDetails &&
          productDetails.productDetails.length > 0
            ? sanitizeHtml(productDetails.productDetails[0].description, {
                allowedTags: [],
                allowedAttributes: {},
              })
            : ''

        const materials =
          productDetails.productDetails &&
          productDetails.productDetails.length == 3
            ? productDetails.productDetails[2].description
            : ''

        const formattedProduct = {
          parent_product_id: product.productId.toString(),
          name: product.productName,
          description: description,
          category:
            `${productDetails?.analytics?.subCategory}/${productDetails?.analytics?.articleType}` ||
            'Apparel',
          retailer_domain: 'myntra.com',
          brand: product.brand,
          gender: product.gender || 'Unisex',
          materials, // Myntra doesn't provide material info in search results
          return_policy_link: 'https://www.myntra.com/returnpolicy',
          return_policy:
            productDetails.serviceability &&
            productDetails.serviceability.descriptors &&
            productDetails.serviceability.descriptors.length > 0
              ? productDetails.serviceability.descriptors[1]
              : '',
          size_chart: '',
          available_bank_offers: JSON.stringify(productDetails.offers || []),
          available_coupons: product.couponData
            ? JSON.stringify(product.couponData)
            : '',
          variants: [],
          operation_type: 'INSERT',
          source: 'myntra',
        }

        // If color variants exist, create size × color matrix
        if (colorVariants.length > 0) {
          console.log(
            `Creating ${sizes.length} sizes × ${
              colorVariants.length
            } colors = ${sizes.length * colorVariants.length} variants`
          )

          for (const colorVariant of colorVariants) {
            // Get color variant specific pricing
            const colorOriginalPrice = colorVariant.price?.mrp
              ? colorVariant.price.mrp
              : originalPrice
            const colorSellingPrice = colorVariant.price?.discounted
              ? colorVariant.price.discounted
              : sellingPrice
            const colorDiscount = this.calculateDiscount(
              colorOriginalPrice,
              colorSellingPrice
            )
            const colorIsOnSale = colorDiscount > 0

            // Get color variant image
            const colorImage =
              colorVariant.defaultImage?.secureSrc ||
              colorVariant.defaultImage?.src ||
              primaryImage

            // Fetch variant details once per color variant (not per size)
            // let variantDetails = await this.fetchProductDetails(colorVariant.id)
            const variantDescription = ''

            for (const size of sizes) {
              const extractedColor =
                colorVariant.baseColour || product.primaryColour || ''

              const formattedVariant = {
                price_currency: 'INR',
                original_price: colorOriginalPrice,
                link_url: `https://www.myntra.com/${colorVariant.landingPageUrl}`,
                deeplink_url: `https://www.myntra.com/${colorVariant.landingPageUrl}`,
                image_url: colorImage,
                alternate_image_urls: alternateImages,
                variant_description: variantDescription,
                is_on_sale: colorIsOnSale,
                is_in_stock: true, // Myntra shows available products only
                size: size,
                color: extractedColor,
                mpn: uuidv5(
                  `${product.productId}-${extractedColor || 'NO_COLOR'}`,
                  '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
                ), // Generate consistent MPN for same color
                ratings_count: 0,
                average_ratings: 0,
                review_count: 0,
                selling_price: colorSellingPrice,
                sale_price: colorIsOnSale ? colorSellingPrice : null,
                final_price: colorSellingPrice,
                discount: colorDiscount,
                operation_type: 'INSERT',
                variant_id: colorVariant.id,
              }

              formattedProduct.variants.push(formattedVariant)
            }
          }
        } else {
          // No color variants found, create variants with sizes only (fallback)
          console.log(
            `No color variants found, creating ${sizes.length} size-only variants`
          )

          for (const size of sizes) {
            const extractedColor = product.primaryColour || ''
            const formattedVariant = {
              price_currency: 'INR',
              original_price: originalPrice,
              link_url: `https://www.myntra.com/${product.landingPageUrl}`,
              deeplink_url: `https://www.myntra.com/${product.landingPageUrl}`,
              image_url: primaryImage,
              alternate_image_urls: alternateImages,
              is_on_sale: isOnSale,
              is_in_stock: true, // Myntra shows available products only
              size: size,
              color: extractedColor,
              mpn: uuidv5(
                extractedColor || 'NO_COLOR',
                '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
              ), // Generate consistent MPN for same color
              ratings_count:
                (productDetails.ratings && productDetails.ratings.totalCount) ||
                0,
              average_ratings: parseFloat(
                (productDetails.ratings &&
                  productDetails.ratings.averageRating) ||
                  0
              ).toFixed(2),
              review_count:
                (productDetails.ratings &&
                  productDetails.ratings.reviewsCount) ||
                0,
              selling_price: sellingPrice,
              sale_price: isOnSale ? sellingPrice : null,
              final_price: sellingPrice,
              discount: discount,
              operation_type: 'INSERT',
              variant_id: `${product.productId}_${size}`,
            }

            formattedProduct.variants.push(formattedVariant)
          }
        }

        // Save product to MongoDB
        try {
          const mongoResult = await this.saveProductToMongoDB(formattedProduct)
          if (mongoResult.operation === 'INSERT') {
            mongoResults.inserted++
          } else if (mongoResult.operation === 'SKIPPED') {
            mongoResults.skipped++
          }
        } catch (error) {
          console.error(
            `MongoDB save failed for ${formattedProduct.name}:`,
            error.message
          )
          mongoResults.errors++
        }

        formattedProducts.push(formattedProduct)
      } catch (error) {
        console.error(
          `❌ Error processing product ${product.productName}:`,
          error.message
        )
        mongoResults.errors++
        // Continue processing other products
      }
    }

    // Log MongoDB results
    console.log(`\n📊 MongoDB Results for ${categoryName}:`)
    console.log(`  Products inserted: ${mongoResults.inserted}`)
    console.log(`  Products skipped: ${mongoResults.skipped}`)
    console.log(`  Products errors: ${mongoResults.errors}`)
    console.log(`  Duplicates skipped: ${mongoResults.duplicatesSkipped}`)

    return formattedProducts
  }

  async appendToJsonFile(
    products,
    categoryName,
    page,
    totalProcessed,
    totalCategories
  ) {
    try {
      const dirPath = path.join(__dirname, 'output', 'IN', 'myntra-IN')

      // Ensure directory exists
      try {
        await fs.mkdir(dirPath, { recursive: true })
      } catch (error) {
        // Directory already exists or creation failed
      }

      const jsonFilePath = path.join(dirPath, 'catalog-all-categories.json')

      let existingData = {
        products: [],
        pagination_info: {},
        categories_processed: [],
        duplicate_tracking: {},
      }

      // Try to read existing file
      try {
        const existingContent = await fs.readFile(jsonFilePath, 'utf8')
        existingData = JSON.parse(existingContent)
      } catch (error) {
        // File doesn't exist yet, start with empty structure
        console.log('Creating new multi-category catalog file...')
      }

      // Append new products
      existingData.products.push(...products)

      // Update categories processed tracking
      if (!existingData.categories_processed.includes(categoryName)) {
        existingData.categories_processed.push(categoryName)
      }

      // Update pagination info
      existingData.pagination_info = {
        current_category: categoryName,
        last_page_processed: page,
        total_products_processed: totalProcessed,
        total_categories_processed: existingData.categories_processed.length,
        total_categories: totalCategories,
        last_offset: page === 1 ? 0 : (page - 1) * this.rowsPerPage - 1,
        rows_per_page: this.rowsPerPage,
        timestamp: new Date().toISOString(),
      }

      // Update duplicate tracking
      existingData.duplicate_tracking = {
        total_unique_products: this.processedProductIds.size,
        products_checked: totalProcessed,
      }

      // Write updated data back to file
      await fs.writeFile(
        jsonFilePath,
        JSON.stringify(existingData, null, 2),
        'utf8'
      )
      console.log(`✅ Appended ${products.length} products to ${jsonFilePath}`)
      console.log(`📊 Total products in file: ${existingData.products.length}`)
      console.log(
        `📂 Categories processed: ${existingData.categories_processed.join(
          ', '
        )}`
      )

      return jsonFilePath
    } catch (error) {
      console.error('❌ Error appending to JSON file:', error.message)
      throw error
    }
  }

  async generateFinalOutputFiles() {
    try {
      console.log('\n📝 Generating final output files...')

      const dirPath = path.join(__dirname, 'output', 'IN', 'myntra-IN')
      const jsonFilePath = path.join(dirPath, 'catalog.json')

      // Read the complete paginated catalog
      const catalogData = JSON.parse(await fs.readFile(jsonFilePath, 'utf8'))
      const allProducts = catalogData.products

      if (allProducts.length === 0) {
        console.log('No products to generate final files for')
        return
      }

      // Calculate total variants
      const totalVariants = allProducts.reduce(
        (sum, product) => sum + product.variants.length,
        0
      )

      // Create JSONL file (each product on a separate line)
      const jsonlFilePath = path.join(dirPath, 'catalog.jsonl')
      const jsonlContent = allProducts
        .map((product) => JSON.stringify(product))
        .join('\n')
      await fs.writeFile(jsonlFilePath, jsonlContent, 'utf8')
      console.log(`📄 JSONL catalog generated: ${jsonlFilePath}`)

      // Gzip the JSONL file
      const gzippedFilePath = `${jsonlFilePath}.gz`
      const jsonlBuffer = await fs.readFile(jsonlFilePath)
      const gzippedBuffer = zlib.gzipSync(jsonlBuffer)
      await fs.writeFile(gzippedFilePath, gzippedBuffer)
      console.log(`🗜️  Gzipped JSONL catalog generated: ${gzippedFilePath}`)

      // Log final summary
      console.log(`\n📊 Final Processing Summary:`)
      console.log(`  Total products processed: ${allProducts.length}`)
      console.log(`  Total variants created: ${totalVariants}`)
      console.log(
        `  Average variants per product: ${(
          totalVariants / allProducts.length
        ).toFixed(1)}`
      )
      console.log(
        `  Categories processed: ${
          catalogData.categories_processed?.join(', ') || 'N/A'
        }`
      )
      console.log(
        `  Unique products: ${
          catalogData.duplicate_tracking?.total_unique_products || 0
        }`
      )
      console.log(
        `  Last category: ${catalogData.pagination_info.current_category}`
      )
      console.log(
        `  Last page: ${catalogData.pagination_info.last_page_processed}`
      )
      console.log(`  Last offset: ${catalogData.pagination_info.last_offset}`)

      return {
        jsonPath: jsonFilePath,
        jsonlPath: jsonlFilePath,
        gzippedPath: gzippedFilePath,
        totalProducts: allProducts.length,
        totalVariants: totalVariants,
      }
    } catch (error) {
      console.error('❌ Error generating final output files:', error.message)
      throw error
    }
  }

  async run() {
    try {
      console.log('Starting Enhanced Myntra scraper for ALL categories...')
      console.log('='.repeat(60))
      console.log(`📂 Total categories to process: ${this.categories.length}`)
      console.log(
        `📂 Categories: ${this.categories.map((c) => c.name).join(', ')}`
      )
      console.log('='.repeat(60))

      // Connect to MongoDB
      await connectDB()

      let totalProductsProcessed = 0
      let totalCategoriesProcessed = 0

      // Process each category
      for (const [categoryIndex, category] of this.categories.entries()) {
        // Set current category for session refresh in product detail methods
        this.currentCategory = category

        console.log(
          `\n${'🎯'.repeat(3)} PROCESSING CATEGORY ${categoryIndex + 1}/${
            this.categories.length
          }: ${category.name.toUpperCase()} ${'🎯'.repeat(3)}`
        )

        // Step 1: Establish session for this category
        const sessionEstablished = await this.establishSession(category)

        if (!sessionEstablished) {
          console.log(
            `⚠️  Warning: Could not establish session for ${category.name}, proceeding anyway...`
          )
        }

        let categoryProductsProcessed = 0
        let currentPage = 1
        let hasMorePages = true

        // Paginate through this category
        while (
          hasMorePages &&
          categoryProductsProcessed < this.productsPerCategory
        ) {
          console.log(
            `\n🔄 Processing ${category.name} - page ${currentPage}...`
          )

          // Step 2: Fetch data for current page of this category
          let data = await this.fetchData(category, currentPage)

          // If we get a timeout on page 2+, try one more time with extra delay
          if (
            data.error &&
            data.message &&
            data.message.includes('timeout') &&
            currentPage > 1
          ) {
            console.log(
              `⚠️  Timeout on page ${currentPage}, waiting 10 seconds and retrying once...`
            )
            await new Promise((resolve) => setTimeout(resolve, 10000))
            data = await this.fetchData(category, currentPage)
          }

          // Check if we have products
          if (data.error || !data.products || data.products.length === 0) {
            // Handle timeout errors specifically
            if (
              data.error &&
              data.message &&
              data.message.includes('timeout')
            ) {
              console.log(
                `⚠️  Timeout error for ${category.name} page ${currentPage}`
              )
              console.log(`Trying to continue with next category...`)
              hasMorePages = false
              break
            }

            console.log(data)
            console.log(
              `📄 No more products found for ${category.name} on page ${currentPage}`
            )
            console.log(`🏁 Last page for ${category.name}: ${currentPage - 1}`)
            console.log(
              `🏁 Last offset: ${
                currentPage === 1
                  ? 0
                  : (currentPage - 1 - 1) * this.rowsPerPage - 1
              }`
            )
            hasMorePages = false
            break
          }

          console.log(
            `📦 Found ${data.products.length} products for ${category.name} on page ${currentPage}`
          )

          // Step 4: Format products for current batch
          const formattedProducts = await this.formatMyntraProducts(
            data,
            category.name
          )

          if (formattedProducts.length > 0) {
            categoryProductsProcessed += formattedProducts.length
            totalProductsProcessed += formattedProducts.length

            // Step 5: Append to JSON file
            await this.appendToJsonFile(
              formattedProducts,
              category.name,
              currentPage,
              totalProductsProcessed,
              this.categories.length
            )

            console.log(
              `✅ Processed ${category.name} batch ${currentPage}: ${formattedProducts.length} products`
            )
            console.log(
              `📊 Category total: ${categoryProductsProcessed}/${this.productsPerCategory} | Global total: ${totalProductsProcessed}`
            )
          }

          // Check if we've reached the limit for this category
          if (categoryProductsProcessed >= this.productsPerCategory) {
            console.log(
              `🎯 Reached limit of ${this.productsPerCategory} products for ${category.name}`
            )
            hasMorePages = false
            break
          }

          // Check if this was a partial batch (less than expected rows)
          if (data.products.length < this.rowsPerPage) {
            console.log(
              `📄 Received ${data.products.length} products (less than ${this.rowsPerPage}), likely last page for ${category.name}`
            )
            console.log(`🏁 Last page for ${category.name}: ${currentPage}`)
            console.log(
              `🏁 Last offset: ${
                currentPage === 1 ? 0 : (currentPage - 1) * this.rowsPerPage - 1
              }`
            )
            hasMorePages = false
          }

          currentPage++
        }

        totalCategoriesProcessed++
        console.log(`\n✅ Completed category: ${category.name}`)
        console.log(
          `📊 Products from this category: ${categoryProductsProcessed}`
        )
        console.log(
          `📊 Unique products so far: ${this.processedProductIds.size}`
        )
      }

      // Generate final output files if we processed any products
      let gzippedFilePath = null
      if (totalProductsProcessed > 0) {
        const result = await this.generateFinalOutputFiles()
        gzippedFilePath = result.gzippedPath
      }

      console.log('\n' + '🎉'.repeat(20))
      console.log('📊 FINAL SUMMARY:')
      console.log(
        `  Total categories processed: ${totalCategoriesProcessed}/${this.categories.length}`
      )
      console.log(`  Total products processed: ${totalProductsProcessed}`)
      console.log(`  Unique products: ${this.processedProductIds.size}`)
      console.log(
        `  Duplicates avoided: ${
          totalProductsProcessed - this.processedProductIds.size
        }`
      )
      console.log(`  Rows per page: ${this.rowsPerPage}`)
      console.log(`  Products per category limit: ${this.productsPerCategory}`)
      console.log('🎉 Enhanced Myntra scraper for ALL categories completed! 🎉')
      console.log('🎉'.repeat(20))

      return { jsonFilePath: gzippedFilePath }
    } catch (error) {
      console.error('Fatal error in Enhanced Myntra scraper:', error.message)
      throw error
    } finally {
      // Disconnect from MongoDB
      await disconnectDB()
    }
  }
}

// Run the scraper if this file is executed directly
if (require.main === module) {
  const scraper = new MyntraScraperEnhanced()
  scraper
    .run()
    .then((gzippedFilePath) => {
      if (gzippedFilePath) {
        console.log(`\n📁 Gzipped catalog file created at: ${gzippedFilePath}`)
      } else {
        console.log('\n❌ No products were processed, no file created')
      }
    })
    .catch((error) => {
      console.error('Scraper failed:', error.message)
      process.exit(1)
    })
}

module.exports = MyntraScraperEnhanced
