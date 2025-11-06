const puppeteer = require('puppeteer')
const fs = require('fs').promises

class LevisScraper {
  constructor() {
    this.browser = null
    this.page = null
    this.products = []
    this.visitedUrls = new Set()
  }

  async init() {
    try {
      // Launch browser with appropriate flags
      this.browser = await puppeteer.launch({
        headless: true, // Set to true for production
        // Removed slowMo: 100 as we're removing all delays
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
      })

      this.page = await this.browser.newPage()

      // Set user agent to avoid bot detection
      await this.page.setUserAgent(
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
      )

      // Set viewport
      await this.page.setViewport({ width: 1280, height: 800 })

      // Enable request interception to handle images and stylesheets
      await this.page.setRequestInterception(true)
      this.page.on('request', (req) => {
        if (
          req.resourceType() === 'stylesheet' ||
          req.resourceType() === 'font' ||
          req.resourceType() === 'image'
        ) {
          req.abort()
        } else {
          req.continue()
        }
      })

      console.log('Browser initialized successfully')
    } catch (error) {
      console.error('Failed to initialize browser:', error)
      throw error
    }
  }

  async navigateToPage(url) {
    try {
      console.log(`Navigating to: ${url}`)
      await this.page.goto(url, {
        waitUntil: ['domcontentloaded', 'networkidle2'],
      })

      // Removed await new Promise(r => setTimeout(r, 3000));

      // Check if we need to handle any popups or overlays
      await this.handlePopups()

      return true
    } catch (error) {
      console.error(`Failed to navigate to ${url}:`, error)
      return false
    }
  }

  async handlePopups() {
    try {
      // Handle cookie banner
      const cookieAccept = await this.page.$(
        '[data-testid="accept-all-cookies"]'
      )
      if (cookieAccept) {
        await cookieAccept.click()
        // Removed await new Promise(r => setTimeout(r, 1000));
      }

      // Handle any modal overlays
      const closeModal = await this.page.$(
        '[data-testid="close-modal"], .modal-close, .close-button'
      )
      if (closeModal) {
        await closeModal.click()
        // Removed await new Promise(r => setTimeout(r, 1000));
      }

      // Handle newsletter popup
      const newsletterClose = await this.page.$(
        '.newsletter-popup .close, [aria-label="Close"]'
      )
      if (newsletterClose) {
        await newsletterClose.click()
        // Removed await new Promise(r => setTimeout(r, 1000));
      }
    } catch (error) {
      console.log(
        'No popups to handle or error handling popups:',
        error.message
      )
    }
  }

  async extractProductLinks() {
    try {
      console.log('Extracting product links from current page...')

      // Log the page contents for debugging
      const pageContent = await this.page.content()
      console.log('Current page contents:', pageContent)

      // Wait for product grid to load (this is a necessary wait for content)
      await this.page.waitForSelector('.results-grid')

      // Extract product links using multiple selectors
      const productLinks = await this.page.evaluate(() => {
        const links = []

        // Try multiple selectors for product links
        const selectors = ['.results-grid .cell-image-link']

        for (const selector of selectors) {
          const elements = document.querySelectorAll(selector)
          elements.forEach((element) => {
            const href = element.href
            if (href) {
              links.push(href)
            }
          })
        }

        // Remove duplicates and return
        return [...new Set(links)]
      })

      console.log(`Found ${productLinks.length} product links`)
      return productLinks
    } catch (error) {
      console.error('Error extracting product links:', error)
      return []
    }
  }

  async extractProductDetails(productUrl) {
    try {
      if (this.visitedUrls.has(productUrl)) {
        console.log(`Skipping already visited URL: ${productUrl}`)
        return null
      }

      console.log(`Extracting details from: ${productUrl}`)

      await this.page.goto(productUrl, {
        waitUntil: 'networkidle2',
      })

      // Removed await new Promise(r => setTimeout(r, 2000));

      // Extract product details
      const productDetails = await this.page.evaluate(() => {
        const getTextContent = (selector) => {
          const element = document.querySelector(selector)
          return element ? element.textContent.trim() : ''
        }

        const getAllTextContent = (selector) => {
          const elements = document.querySelectorAll(selector)
          return Array.from(elements)
            .map((el) => el.textContent.trim())
            .filter((text) => text)
        }

        const getAttribute = (selector, attribute) => {
          const element = document.querySelector(selector)
          return element ? element.getAttribute(attribute) : ''
        }

        return {
          url: window.location.href,
          title:
            getTextContent(
              'h1, .product-title, [data-testid="product-title"]'
            ) || document.title.split(' | ')[0],

          price:
            getTextContent(
              '.price, .product-price, [data-testid="price"], .current-price, .price-current'
            ) || getTextContent('.price-display, .product-price-display'),

          originalPrice: getTextContent(
            '.original-price, .price-original, .was-price, .crossed-out-price'
          ),

          description:
            getTextContent(
              '.product-description, .description, [data-testid="description"]'
            ) || getTextContent('.product-details, .product-summary'),

          sku:
            getTextContent('.sku, .product-sku, [data-testid="sku"]') ||
            getAttribute('[data-sku]', 'data-sku'),

          brand: "Levi's",

          category:
            getTextContent(
              '.breadcrumb, .breadcrumbs, nav[aria-label="breadcrumb"]'
            ) || getAllTextContent('.breadcrumb a, .breadcrumbs a').join(' > '),

          images: Array.from(
            document.querySelectorAll(
              'img[src*="product"], .product-image img, .gallery img'
            )
          )
            .map((img) => img.src)
            .filter((src) => src && !src.includes('data:image')),

          colors: getAllTextContent(
            '.color-option, .color-swatch, [data-testid="color"]'
          ),

          sizes: getAllTextContent(
            '.size-option, .size-button, [data-testid="size"], .size-selector option'
          ).filter((size) => size && size !== 'Select Size'),

          features: getAllTextContent(
            '.product-features li, .features li, .product-highlights li'
          ),

          materials: getAllTextContent(
            '.materials, .fabric-info, .product-materials'
          ),

          careInstructions: getTextContent('.care-instructions, .care-info'),

          availability: getTextContent(
            '.availability, .stock-status, [data-testid="availability"]'
          ),

          rating: getTextContent('.rating, .stars, .review-rating'),

          reviewCount: getTextContent('.review-count, .reviews-count'),

          productId:
            getAttribute('[data-product-id]', 'data-product-id') ||
            window.location.pathname.split('/').pop(),

          tags: getAllTextContent('.product-tags li, .tags li'),

          fit: getTextContent('.fit-info, .fit-type, [data-testid="fit"]'),

          rise: getTextContent('.rise-info, .rise-type'),

          inseam: getTextContent('.inseam-info, .inseam'),

          waist: getTextContent('.waist-info, .waist'),

          leg: getTextContent('.leg-opening, .leg-info'),

          scrapedAt: new Date().toISOString(),
        }
      })

      // Clean up the data
      Object.keys(productDetails).forEach((key) => {
        if (Array.isArray(productDetails[key])) {
          productDetails[key] = productDetails[key].filter(
            (item) => item && item.trim()
          )
        } else if (typeof productDetails[key] === 'string') {
          productDetails[key] = productDetails[key].trim()
        }
      })

      this.visitedUrls.add(productUrl)
      console.log(`Successfully extracted details for: ${productDetails.title}`)

      return productDetails
    } catch (error) {
      console.error(
        `Error extracting product details from ${productUrl}:`,
        error
      )
      return null
    }
  }

  async scrapePage(url) {
    try {
      const success = await this.navigateToPage(url)
      if (!success) {
        console.log(`Skipping page due to navigation failure: ${url}`)
        return
      }

      const productLinks = await this.extractProductLinks()

      console.log(`Processing ${productLinks.length} products from ${url}`)
      console.log(productLinks)

      for (let i = 0; i < productLinks.length; i++) {
        const productUrl = productLinks[i]
        console.log(
          `Processing product ${i + 1}/${productLinks.length}: ${productUrl}`
        )

        const productDetails = await this.extractProductDetails(productUrl)

        if (productDetails) {
          this.products.push(productDetails)
          console.log(`Total products collected: ${this.products.length}`)
        }

        // Removed await new Promise(r => setTimeout(r, 1000 + Math.random() * 2000));
      }
    } catch (error) {
      console.error(`Error scraping page ${url}:`, error)
    }
  }

  async saveData() {
    try {
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
      const filename = `levis-products-${timestamp}.json`

      const data = {
        scrapedAt: new Date().toISOString(),
        totalProducts: this.products.length,
        products: this.products,
      }

      await fs.writeFile(filename, JSON.stringify(data, null, 2))
      console.log(`Data saved to ${filename}`)

      // Also save a summary
      const summary = {
        totalProducts: this.products.length,
        categories: [
          ...new Set(this.products.map((p) => p.category).filter((c) => c)),
        ],
        priceRange: {
          min: Math.min(
            ...this.products
              .map((p) => parseFloat(p.price?.replace(/[^0-9.]/g, '')) || 0)
              .filter((p) => p > 0)
          ),
          max: Math.max(
            ...this.products.map(
              (p) => parseFloat(p.price?.replace(/[^0-9.]/g, '')) || 0
            )
          ),
        },
        scrapedAt: new Date().toISOString(),
      }

      await fs.writeFile(
        `levis-summary-${timestamp}.json`,
        JSON.stringify(summary, null, 2)
      )
      console.log(`Summary saved to levis-summary-${timestamp}.json`)
    } catch (error) {
      console.error('Error saving data:', error)
    }
  }

  async run() {
    try {
      await this.init()

      const urls = [
        'https://www.levi.com/US/en_US/clothing/men/c/levi_clothing_men',
      ]

      for (const url of urls) {
        console.log(`\n=== Starting to scrape: ${url} ===`)
        await this.scrapePage(url)
        console.log(`=== Finished scraping: ${url} ===\n`)

        // Removed await new Promise(r => setTimeout(r, 3000));
      }

      console.log(
        `\n=== Scraping completed! Total products: ${this.products.length} ===`
      )
      await this.saveData()
    } catch (error) {
      console.error('Error in main execution:', error)
    } finally {
      if (this.browser) {
        await this.browser.close()
        console.log('Browser closed')
      }
    }
  }
}

// Run the scraper
const scraper = new LevisScraper()
scraper.run().catch(console.error)

module.exports = LevisScraper
