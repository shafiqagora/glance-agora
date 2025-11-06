const SFTPHelper = require('./utils/sftpHelper')
const { processAllStores, main } = require('./shopify-crawler-csv')
const { main: everlaneMain } = require('./everlane-crawler')
const { main: goodAmericanMain } = require('./good-american-crawler')
const { main: walmartMain } = require('./walmart-crawler')
const { main: zaraMain } = require('./zara-men-products-scraper')
const { main: lulusMain } = require('./lulus-crawler')
const { main: lululemonMain } = require('./lululemon-crawler')
const { main: freepeopleMain } = require('./freepeople-crawler')
const { main: gapMain } = require('./gap-crawler')
const { main: nikeMain } = require('./nike-scraper')
const { main: adidasMain } = require('./adidas-scraper')
const { main: hmMain } = require('./hm-scraper')
const { main: abercrombieMain } = require('./abercrombie-scraper')
const { main: aritziaMain } = require('./aritizia-scraper')
const { main: jcrewMain } = require('./jcrew-scraper')
const MyntraScraperEnhanced = require('./myntra-scraper-enhanced')
const { processStoresFromServer } = require('./shopify-recrawler-csv')
const {
  processStoresFromServer: processEverlaneRecrawler,
} = require('./everlane-recrawler')
const {
  processStoresFromServer: processGoodAmericanRecrawler,
} = require('./good-american-recrawler')
const {
  processStoresFromServer: processFreePeopleRecrawler,
} = require('./freepeople-recrawler')
const {
  processStoresFromServer: processLulusRecrawler,
} = require('./lulus-recrawler')
const {
  processStoresFromServer: processAritziaRecrawler,
} = require('./aritizia-recrawler')
const {
  processStoresFromServer: processLululemonRecrawler,
} = require('./lululemon-recrawler')
const fs = require('fs')
const path = require('path')

/**
 * Upload results from the main crawler
 */
async function uploadCrawlerResults() {
  try {
    console.log('🚀 Starting crawler and upload process...')

    // Run the main crawler
    console.log('\n1️⃣ Running main crawler...')
    const crawlerResults = await processAllStores()

    if (!crawlerResults || crawlerResults.successful.length === 0) {
      console.log('❌ No successful crawls to upload')
      return
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = crawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      crawler_results: crawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_crawled: crawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(__dirname, 'crawl-and-upload-results.json')
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Crawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(`  Stores crawled: ${combinedResults.summary.stores_crawled}`)
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error('❌ Error in crawl and upload process:', error.message)
    throw error
  }
}

/**
 * Upload a single store's catalog
 */
async function uploadSingleStore(storeUrl, countryCode = 'US') {
  try {
    console.log(`🏪 Processing single store: ${storeUrl}`)

    console.log('🆕 Running initial crawl...')
    const result = await main(storeUrl, countryCode, false)

    if (!result) {
      console.log('❌ Crawl failed, nothing to upload')
      return
    }

    const filePaths = {
      csvPath: result.csvPath,
      jsonPath: result.jsonPath,
    }

    // Extract domain for store info
    const domain = storeUrl
      .replace(/^https?:\/\//, '')
      .replace(/^www\./, '')
      .split('/')[0]

    const storeInfo = {
      brandName: domain,
      url: storeUrl,
      region: countryCode,
      countryCode: countryCode,
    }

    // Upload to SFTP
    console.log('\n📤 Uploading to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Single store upload completed!')
    return uploadResult
  } catch (error) {
    console.error('❌ Error in single store upload:', error.message)
    throw error
  }
}

/**
 * Test SFTP connection
 */
async function testSFTPConnection() {
  const sftp = new SFTPHelper()
  return await sftp.testConnection()
}

/**
 * Process and upload Everlane catalog
 */
async function processEverlaneCatalog() {
  try {
    console.log('🏪 Processing Everlane catalog...')

    // Run the Everlane crawler
    console.log('🆕 Running Everlane crawler...')
    const API_URL = 'https://ac.cnstrc.com/browse/collection_id/womens-all'
    const returnPolicy =
      'https://support.everlane.com/what-is-your-return-policy-H1fMnra0s'

    const result = await everlaneMain(API_URL, returnPolicy)

    if (!result) {
      console.log('❌ Everlane crawl failed, nothing to upload')
      return
    }

    const filePaths = {
      jsonPath: result.jsonPath,
    }

    const storeInfo = {
      brandName: 'everlane',
      url: 'https://www.everlane.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Everlane catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Everlane catalog upload completed!')
    return uploadResult
  } catch (error) {
    console.error('❌ Error in Everlane catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Good American catalog
 */
async function processGoodAmericanCatalog() {
  try {
    console.log('🏪 Processing Good American catalog...')

    // Run the Good American crawler
    console.log('🆕 Running Good American crawler...')
    const API_URL = 'https://www.goodamerican.com/en-US/api/searchspring'
    const returnPolicy =
      'https://www.goodamerican.com/pages/returns-info?srsltid=AfmBOopLPZQN2NRiAOPocYzmFnrh0G6Md8RsYQAXDKgia-GUO9KstEtU'

    const result = await goodAmericanMain(API_URL, returnPolicy)

    if (!result) {
      console.log('❌ Good American crawl failed, nothing to upload')
      return
    }

    const filePaths = {
      jsonPath: result.jsonPath,
    }

    const storeInfo = {
      brandName: 'good_american',
      url: 'https://www.goodamerican.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Good American catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Good American catalog upload completed!')
    return uploadResult
  } catch (error) {
    console.error('❌ Error in Good American catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Walmart catalog
 */
async function processWalmartCatalog(catId = '5438', maxPages = 1) {
  try {
    console.log('🏪 Processing Walmart catalog...')

    // Run the Walmart crawler
    console.log('🆕 Running Walmart crawler...')
    console.log(`📋 Category ID: ${catId}, Max Pages: ${maxPages}`)

    const result = await walmartMain(catId, maxPages)

    if (!result) {
      console.log('❌ Walmart crawl failed, nothing to upload')
      return
    }

    const filePaths = {
      jsonPath: result.jsonPath,
    }

    const storeInfo = {
      brandName: 'walmart',
      url: 'https://www.walmart.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Walmart catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Walmart catalog upload completed!')
    return uploadResult
  } catch (error) {
    console.error('❌ Error in Walmart catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Myntra catalog
 */
async function processMyntraCatalog() {
  try {
    console.log('🏪 Processing Myntra catalog...')

    // Run the Myntra scraper
    console.log('🆕 Running Myntra scraper...')
    const scraper = new MyntraScraperEnhanced()
    const result = await scraper.run()

    if (!result || !result.jsonFilePath) {
      console.log('❌ Myntra scrape failed, nothing to upload')
      return
    }

    const filePaths = {
      jsonPath: result.jsonFilePath,
    }

    const storeInfo = {
      brandName: 'myntra',
      url: 'https://www.myntra.com',
      region: 'IN',
      countryCode: 'IN',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Myntra catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Myntra catalog upload completed!')
    return uploadResult
  } catch (error) {
    console.error('❌ Error in Myntra catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Zara catalog (Men's and Women's combined)
 */
async function processZaraCatalog() {
  try {
    console.log("🏪 Processing Zara catalog (Men's & Women's)...")

    // Run the Zara scraper
    console.log('🆕 Running Zara scraper for both categories...')
    const results = await zaraMain()

    if (!results || results.length === 0) {
      console.log('❌ Zara scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with both categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'zara',
      url: 'https://www.zara.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Zara catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Zara catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from both Men's and Women's categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Zara catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Lulus catalog
 */
async function processLulusCatalog() {
  try {
    console.log('🏪 Processing Lulus catalog...')

    // Run the Lulus scraper
    console.log('🆕 Running Lulus scraper...')
    const results = await lulusMain()

    if (!results || results.length === 0) {
      console.log('❌ Lulus scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'lulus',
      url: 'https://www.lulus.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Lulus catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Lulus catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Lulus catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Lululemon catalog
 */
async function processLululemonCatalog() {
  try {
    console.log('🏪 Processing Lululemon catalog...')

    // Run the Lululemon scraper
    console.log('🆕 Running Lululemon scraper...')
    const results = await lululemonMain()

    if (!results || results.length === 0) {
      console.log('❌ Lululemon scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'lululemon',
      url: 'https://shop.lululemon.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Lululemon catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Lululemon catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Lululemon catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload FreePeople catalog
 */
async function processFreePeopleCatalog() {
  try {
    console.log('🏪 Processing FreePeople catalog...')

    // Run the FreePeople scraper
    console.log('🆕 Running FreePeople scraper...')
    const results = await freepeopleMain()

    if (!results || results.length === 0) {
      console.log('❌ FreePeople scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'freepeople',
      url: 'https://www.freepeople.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading FreePeople catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ FreePeople catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in FreePeople catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Gap catalog
 */
async function processGapCatalog() {
  try {
    console.log('🏪 Processing Gap catalog...')

    // Run the Gap scraper
    console.log('🆕 Running Gap scraper...')
    const results = await gapMain()

    if (!results || results.length === 0) {
      console.log('❌ Gap scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'gap',
      url: 'https://www.gap.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Gap catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Gap catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Gap catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Nike catalog
 */
async function processNikeCatalog() {
  try {
    console.log('🏪 Processing Nike catalog...')

    // Run the Nike scraper
    console.log('🆕 Running Nike scraper...')
    const results = await nikeMain()

    if (!results || results.length === 0) {
      console.log('❌ Nike scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'nike',
      url: 'https://www.nike.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Nike catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Nike catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Nike catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Adidas catalog
 */
async function processAdidasCatalog() {
  try {
    console.log('🏪 Processing Adidas catalog...')

    // Run the Adidas scraper
    console.log('🆕 Running Adidas scraper...')
    const results = await adidasMain()

    if (!results || results.length === 0) {
      console.log('❌ Adidas scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'adidas',
      url: 'https://www.adidas.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Adidas catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Adidas catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Adidas catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload H&M catalog
 */
async function processHMCatalog() {
  try {
    console.log('🏪 Processing H&M catalog...')

    // Run the H&M scraper
    console.log('🆕 Running H&M scraper...')
    const results = await hmMain()

    if (!results || results.length === 0) {
      console.log('❌ H&M scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'hm',
      url: 'https://www2.hm.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading H&M catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ H&M catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in H&M catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload Abercrombie & Fitch catalog
 */
async function processAbercrombieAndFitchCatalog() {
  try {
    console.log('🏪 Processing Abercrombie & Fitch catalog...')

    // Run the Abercrombie & Fitch scraper
    console.log('🆕 Running Abercrombie & Fitch scraper...')
    const results = await abercrombieMain()

    if (!results || results.length === 0) {
      console.log('❌ Abercrombie & Fitch scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'abercrombie',
      url: 'https://www.abercrombie.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Abercrombie & Fitch catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Abercrombie & Fitch catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error(
      '❌ Error in Abercrombie & Fitch catalog upload:',
      error.message
    )
    throw error
  }
}

/**
 * Process and upload Aritzia catalog
 */
async function processAritziaCatalog() {
  try {
    console.log('🏪 Processing Aritzia catalog...')

    // Run the Aritzia scraper
    console.log('🆕 Running Aritzia scraper...')
    const results = await aritziaMain()

    if (!results || results.length === 0) {
      console.log('❌ Aritzia scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'aritzia',
      url: 'https://www.aritzia.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading Aritzia catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ Aritzia catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in Aritzia catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload J.Crew catalog
 */
async function processJCrewCatalog() {
  try {
    console.log('🏪 Processing J.Crew catalog...')

    // Run the J.Crew scraper
    console.log('🆕 Running J.Crew scraper...')
    const results = await jcrewMain()

    if (!results || results.length === 0) {
      console.log('❌ J.Crew scrape failed, nothing to upload')
      return
    }

    // Get the combined result (should be only one result with all categories)
    const combinedResult = results[0]

    const filePaths = {
      jsonPath: combinedResult.jsonPath,
    }

    const storeInfo = {
      brandName: 'jcrew',
      url: 'https://www.jcrew.com',
      region: 'US',
      countryCode: 'US',
    }

    // Upload to SFTP
    console.log('\n📤 Uploading J.Crew catalog to SFTP server...')
    const sftp = new SFTPHelper()
    const uploadResult = await sftp.uploadStoreCatalog(storeInfo, filePaths)
    await sftp.disconnect()

    console.log('✅ J.Crew catalog upload completed!')
    console.log(
      `📊 Uploaded ${combinedResult.totalProducts} products from all categories`
    )

    return uploadResult
  } catch (error) {
    console.error('❌ Error in J.Crew catalog upload:', error.message)
    throw error
  }
}

/**
 * Process and upload all custom crawler catalogs (Everlane + Good American + Walmart + Myntra + Zara + Lulus + Lululemon + FreePeople + Gap + Nike + Adidas + H&M + Abercrombie + Aritzia + J.Crew)
 */
async function processAllCustomCatalogs() {
  try {
    console.log('🚀 Starting custom crawler and upload process...')

    const results = {
      everlane: null,
      goodAmerican: null,
      walmart: null,
      myntra: null,
      zara: null,
      lulus: null,
      lululemon: null,
      freepeople: null,
      gap: null,
      nike: null,
      adidas: null,
      hm: null,
      abercrombie: null,
      aritzia: null,
      jcrew: null,
      successful: [],
      failed: [],
    }

    // Process Everlane
    try {
      console.log('\n1️⃣ Processing Everlane...')
      results.everlane = await processEverlaneCatalog()
      results.successful.push('everlane')
      console.log('✅ Everlane processed successfully')
    } catch (error) {
      console.error('❌ Everlane processing failed:', error.message)
      results.failed.push({ store: 'everlane', error: error.message })
    }

    // Process Good American
    try {
      console.log('\n2️⃣ Processing Good American...')
      results.goodAmerican = await processGoodAmericanCatalog()
      results.successful.push('good_american')
      console.log('✅ Good American processed successfully')
    } catch (error) {
      console.error('❌ Good American processing failed:', error.message)
      results.failed.push({ store: 'good_american', error: error.message })
    }

    // Process Walmart
    try {
      console.log('\n3️⃣ Processing Walmart...')
      results.walmart = await processWalmartCatalog('5438', 1) // Default category and pages
      results.successful.push('walmart')
      console.log('✅ Walmart processed successfully')
    } catch (error) {
      console.error('❌ Walmart processing failed:', error.message)
      results.failed.push({ store: 'walmart', error: error.message })
    }

    // Process Myntra
    try {
      console.log('\n4️⃣ Processing Myntra...')
      results.myntra = await processMyntraCatalog()
      results.successful.push('myntra')
      console.log('✅ Myntra processed successfully')
    } catch (error) {
      console.error('❌ Myntra processing failed:', error.message)
      results.failed.push({ store: 'myntra', error: error.message })
    }

    // Process Zara
    try {
      console.log('\n5️⃣ Processing Zara...')
      results.zara = await processZaraCatalog()
      results.successful.push('zara')
      console.log('✅ Zara processed successfully')
    } catch (error) {
      console.error('❌ Zara processing failed:', error.message)
      results.failed.push({ store: 'zara', error: error.message })
    }

    // Process Lulus
    try {
      console.log('\n6️⃣ Processing Lulus...')
      results.lulus = await processLulusCatalog()
      results.successful.push('lulus')
      console.log('✅ Lulus processed successfully')
    } catch (error) {
      console.error('❌ Lulus processing failed:', error.message)
      results.failed.push({ store: 'lulus', error: error.message })
    }

    // Process Lululemon
    try {
      console.log('\n7️⃣ Processing Lululemon...')
      results.lululemon = await processLululemonCatalog()
      results.successful.push('lululemon')
      console.log('✅ Lululemon processed successfully')
    } catch (error) {
      console.error('❌ Lululemon processing failed:', error.message)
      results.failed.push({ store: 'lululemon', error: error.message })
    }

    // Process FreePeople
    try {
      console.log('\n8️⃣ Processing FreePeople...')
      results.freepeople = await processFreePeopleCatalog()
      results.successful.push('freepeople')
      console.log('✅ FreePeople processed successfully')
    } catch (error) {
      console.error('❌ FreePeople processing failed:', error.message)
      results.failed.push({ store: 'freepeople', error: error.message })
    }

    // Process Gap
    try {
      console.log('\n9️⃣ Processing Gap...')
      results.gap = await processGapCatalog()
      results.successful.push('gap')
      console.log('✅ Gap processed successfully')
    } catch (error) {
      console.error('❌ Gap processing failed:', error.message)
      results.failed.push({ store: 'gap', error: error.message })
    }

    // Process Nike
    try {
      console.log('\n🔟 Processing Nike...')
      results.nike = await processNikeCatalog()
      results.successful.push('nike')
      console.log('✅ Nike processed successfully')
    } catch (error) {
      console.error('❌ Nike processing failed:', error.message)
      results.failed.push({ store: 'nike', error: error.message })
    }

    // Process Adidas
    try {
      console.log('\n1️⃣1️⃣ Processing Adidas...')
      results.adidas = await processAdidasCatalog()
      results.successful.push('adidas')
      console.log('✅ Adidas processed successfully')
    } catch (error) {
      console.error('❌ Adidas processing failed:', error.message)
      results.failed.push({ store: 'adidas', error: error.message })
    }

    // Process H&M
    try {
      console.log('\n1️⃣2️⃣ Processing H&M...')
      results.hm = await processHMCatalog()
      results.successful.push('hm')
      console.log('✅ H&M processed successfully')
    } catch (error) {
      console.error('❌ H&M processing failed:', error.message)
      results.failed.push({ store: 'hm', error: error.message })
    }

    // Process Abercrombie & Fitch
    try {
      console.log('\n1️⃣3️⃣ Processing Abercrombie & Fitch...')
      results.abercrombie = await processAbercrombieAndFitchCatalog()
      results.successful.push('abercrombie')
      console.log('✅ Abercrombie & Fitch processed successfully')
    } catch (error) {
      console.error('❌ Abercrombie & Fitch processing failed:', error.message)
      results.failed.push({ store: 'abercrombie', error: error.message })
    }

    // Process Aritzia
    try {
      console.log('\n1️⃣4️⃣ Processing Aritzia...')
      results.aritzia = await processAritziaCatalog()
      results.successful.push('aritzia')
      console.log('✅ Aritzia processed successfully')
    } catch (error) {
      console.error('❌ Aritzia processing failed:', error.message)
      results.failed.push({ store: 'aritzia', error: error.message })
    }

    // Process J.Crew
    try {
      console.log('\n1️⃣5️⃣ Processing J.Crew...')
      results.jcrew = await processJCrewCatalog()
      results.successful.push('jcrew')
      console.log('✅ J.Crew processed successfully')
    } catch (error) {
      console.error('❌ J.Crew processing failed:', error.message)
      results.failed.push({ store: 'jcrew', error: error.message })
    }

    // Save results
    const resultsPath = path.join(__dirname, 'custom-crawl-results.json')
    fs.writeFileSync(resultsPath, JSON.stringify(results, null, 2))

    console.log('\n🎉 Custom crawler process completed!')
    console.log(`📊 Summary:`)
    console.log(`  Successful: ${results.successful.length}`)
    console.log(`  Failed: ${results.failed.length}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return results
  } catch (error) {
    console.error('❌ Error in custom crawler process:', error.message)
    throw error
  }
}

/**
 * Process and upload Shopify recrawler results
 */
async function processShopifyRecrawler() {
  try {
    console.log('🔄 Starting Shopify recrawler and upload process...')

    // Run the Shopify recrawler
    console.log('\n1️⃣ Running Shopify recrawler...')
    const recrawlerResults = await processStoresFromServer()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'shopify-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Shopify recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in Shopify recrawl and upload process:',
      error.message
    )
    throw error
  }
}
async function uploadShopifyRecrawler() {
  try {
    console.log('🔄 Starting Shopify recrawler and upload process...')

    // Run the Shopify recrawler
    let results = require('./recrawl-processing-results.json')

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = results.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      upload_results: uploadResults,
      summary: {
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'shopify-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Shopify recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error('❌ Error in Shopify upload process:', error.message)
    throw error
  }
}

/**
 * Process and upload Everlane recrawler results
 */
async function processEverlaneRecrawlerAndUpload() {
  try {
    console.log('🔄 Starting Everlane recrawler and upload process...')

    // Run the Everlane recrawler
    console.log('\n1️⃣ Running Everlane recrawler...')
    const recrawlerResults = await processEverlaneRecrawler()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled Everlane files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'everlane-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Everlane recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in Everlane recrawl and upload process:',
      error.message
    )
    throw error
  }
}

/**
 * Process and upload Good American recrawler results
 */
async function processGoodAmericanRecrawlerAndUpload() {
  try {
    console.log('🔄 Starting Good American recrawler and upload process...')

    // Run the Good American recrawler
    console.log('\n1️⃣ Running Good American recrawler...')
    const recrawlerResults = await processGoodAmericanRecrawler()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log(
      '\n2️⃣ Uploading recrawled Good American files to SFTP server...'
    )
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'good-american-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Good American recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in Good American recrawl and upload process:',
      error.message
    )
    throw error
  }
}

/**
 * Process and upload FreePeople recrawler results
 */
async function processFreePeopleRecrawlerAndUpload() {
  try {
    console.log('🔄 Starting FreePeople recrawler and upload process...')

    // Run the FreePeople recrawler
    console.log('\n1️⃣ Running FreePeople recrawler...')
    const recrawlerResults = await processFreePeopleRecrawler()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled FreePeople files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'freepeople-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 FreePeople recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in FreePeople recrawl and upload process:',
      error.message
    )
    throw error
  }
}

/**
 * Process and upload Lulus recrawler results
 */
async function processLulusRecrawlerAndUpload() {
  try {
    console.log('🔄 Starting Lulus recrawler and upload process...')

    // Run the Lulus recrawler
    console.log('\n1️⃣ Running Lulus recrawler...')
    const recrawlerResults = await processLulusRecrawler()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled Lulus files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'lulus-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Lulus recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in Lulus recrawl and upload process:',
      error.message
    )
    throw error
  }
}

/**
 * Process and upload Aritzia recrawler results
 */
async function processAritziaRecrawlerAndUpload() {
  try {
    console.log('🔄 Starting Aritzia recrawler and upload process...')

    // Run the Aritzia recrawler
    console.log('\n1️⃣ Running Aritzia recrawler...')
    const recrawlerResults = await processAritziaRecrawler()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled Aritzia files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'aritzia-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Aritzia recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in Aritzia recrawl and upload process:',
      error.message
    )
    throw error
  }
}

/**
 * Process and upload Lululemon recrawler results
 */
async function processLululemonRecrawlerAndUpload() {
  try {
    console.log('🔄 Starting Lululemon recrawler and upload process...')

    // Run the Lululemon recrawler
    console.log('\n1️⃣ Running Lululemon recrawler...')
    const recrawlerResults = await processLululemonRecrawler()

    if (!recrawlerResults || recrawlerResults.successful.length === 0) {
      console.log('❌ No successful recrawls to upload')
      return recrawlerResults
    }

    // Prepare data for SFTP upload
    const sftp = new SFTPHelper()
    const storesToUpload = recrawlerResults.successful.map((store) => ({
      brandName: store.brandName,
      url: store.url,
      region: store.region,
      countryCode: store.region,
      filePaths: {
        jsonPath: store.jsonPath,
      },
    }))

    // Upload to SFTP
    console.log('\n2️⃣ Uploading recrawled Lululemon files to SFTP server...')
    const uploadResults = await sftp.uploadMultipleStores(storesToUpload)

    // Save combined results
    const combinedResults = {
      recrawler_results: recrawlerResults,
      upload_results: uploadResults,
      summary: {
        stores_recrawled: recrawlerResults.successful.length,
        stores_uploaded: uploadResults.successful.length,
        upload_failed: uploadResults.failed.length,
        recrawl_failed: recrawlerResults.failed.length,
        recrawl_skipped: recrawlerResults.skipped.length,
        completed_at: new Date().toISOString(),
      },
    }

    const resultsPath = path.join(
      __dirname,
      'lululemon-recrawl-and-upload-results.json'
    )
    fs.writeFileSync(resultsPath, JSON.stringify(combinedResults, null, 2))

    console.log('\n🎉 Lululemon recrawl and upload process completed!')
    console.log(`📊 Summary:`)
    console.log(
      `  Stores recrawled: ${combinedResults.summary.stores_recrawled}`
    )
    console.log(`  Stores uploaded: ${combinedResults.summary.stores_uploaded}`)
    console.log(`  Upload failures: ${combinedResults.summary.upload_failed}`)
    console.log(`  Recrawl failures: ${combinedResults.summary.recrawl_failed}`)
    console.log(`  Recrawl skipped: ${combinedResults.summary.recrawl_skipped}`)
    console.log(`📄 Detailed results: ${resultsPath}`)

    return combinedResults
  } catch (error) {
    console.error(
      '❌ Error in Lululemon recrawl and upload process:',
      error.message
    )
    throw error
  }
}

/**
 * Upload existing catalog.jsonl.gz file to SFTP server
 */
async function uploadExistingCatalogFile() {
  try {
    console.log('📤 Uploading existing catalog.jsonl.gz file to SFTP server...')

    const catalogFilePath = path.join(__dirname, 'catalog.jsonl.gz')

    // Check if the file exists
    if (!fs.existsSync(catalogFilePath)) {
      console.log('❌ catalog.jsonl.gz file not found in root directory')
      return { success: false, error: 'File not found' }
    }

    // Get file stats
    const stats = fs.statSync(catalogFilePath)
    console.log(`📄 File size: ${(stats.size / 1024).toFixed(2)} KB`)

    // Create SFTP helper instance
    const sftp = new SFTPHelper()

    // Get current date in day-month-year format
    const now = new Date()
    const day = now.getDate().toString().padStart(2, '0')
    const monthNames = [
      'JAN',
      'FEB',
      'MAR',
      'APR',
      'MAY',
      'JUN',
      'JUL',
      'AUG',
      'SEP',
      'OCT',
      'NOV',
      'DEC',
    ]
    const month = monthNames[now.getMonth()]
    const year = now.getFullYear()
    const dateFolder = `${day}-${month}-${year}`

    // Define the remote path with current date
    const remotePath = `${dateFolder}/US/zara-US/catalog.jsonl.gz`

    console.log(`📤 Uploading to: ${remotePath}`)

    // Upload the file
    const uploadResult = await sftp.uploadExistingCatalogFile(
      catalogFilePath,
      remotePath
    )

    // Disconnect from SFTP
    await sftp.disconnect()

    if (uploadResult.success) {
      console.log('✅ Catalog file uploaded successfully!')
      console.log(
        `📊 Uploaded ${(stats.size / 1024).toFixed(2)} KB to ${remotePath}`
      )
    } else {
      console.log('❌ Failed to upload catalog file:', uploadResult.error)
    }

    return uploadResult
  } catch (error) {
    console.error('❌ Error uploading existing catalog file:', error.message)
    throw error
  }
}

// Export functions
module.exports = {
  uploadCrawlerResults,
  uploadSingleStore,
  testSFTPConnection,
  processAllCustomCatalogs,
  processEverlaneCatalog,
  processGoodAmericanCatalog,
  processWalmartCatalog,
  processMyntraCatalog,
  processZaraCatalog,
  processLulusCatalog,
  processLululemonCatalog,
  processFreePeopleCatalog,
  processGapCatalog,
  processNikeCatalog,
  processAdidasCatalog,
  processHMCatalog,
  processAbercrombieAndFitchCatalog,
  processAritziaCatalog,
  processJCrewCatalog,
  processShopifyRecrawler,
  processEverlaneRecrawlerAndUpload,
  processGoodAmericanRecrawlerAndUpload,
  processFreePeopleRecrawlerAndUpload,
  processLulusRecrawlerAndUpload,
  processAritziaRecrawlerAndUpload,
  processLululemonRecrawlerAndUpload,
  uploadExistingCatalogFile,
}

// Command line interface
if (require.main === module) {
  const args = process.argv.slice(2)
  const command = args[0]

  switch (command) {
    case 'crawl':
      console.log('📋 Running crawl and upload...')
      uploadCrawlerResults()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl':
      console.log('🔄 Running Shopify recrawl and upload...')
      processShopifyRecrawler()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break
    case 'upload-recrawl':
      console.log('🔄 Running Shopify recrawl and upload...')
      uploadShopifyRecrawler()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl-everlane':
      console.log('🔄 Running Everlane recrawl and upload...')
      processEverlaneRecrawlerAndUpload()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl-good-american':
      console.log('🔄 Running Good American recrawl and upload...')
      processGoodAmericanRecrawlerAndUpload()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl-freepeople':
      console.log('🔄 Running FreePeople recrawl and upload...')
      processFreePeopleRecrawlerAndUpload()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl-lulus':
      console.log('🔄 Running Lulus recrawl and upload...')
      processLulusRecrawlerAndUpload()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl-aritzia':
      console.log('🔄 Running Aritzia recrawl and upload...')
      processAritziaRecrawlerAndUpload()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'recrawl-lululemon':
      console.log('🔄 Running Lululemon recrawl and upload...')
      processLululemonRecrawlerAndUpload()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'store':
      if (args.length < 2) {
        console.log(
          '❌ Usage: node upload-catalogs.js store <store-url> [country-code]'
        )
        process.exit(1)
      }

      const storeUrl = args[1]
      const countryCode = args[2] || 'US'

      uploadSingleStore(storeUrl, countryCode)
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'test':
      console.log('🧪 Testing SFTP connection...')
      testSFTPConnection()
        .then((success) => {
          process.exit(success ? 0 : 1)
        })
        .catch((error) => {
          console.error('❌ Test failed:', error.message)
          process.exit(1)
        })
      break

    case 'custom':
      console.log('📋 Running custom crawler and upload...')
      processAllCustomCatalogs()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'everlane':
      console.log('🏪 Running Everlane crawler and upload...')
      processEverlaneCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'good-american':
      console.log('🏪 Running Good American crawler and upload...')
      processGoodAmericanCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'walmart':
      const catId = '5438'
      const maxPages = 10

      processWalmartCatalog(catId, maxPages)
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'myntra':
      console.log('🏪 Running Myntra scraper and upload...')
      processMyntraCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'zara':
      console.log('🏪 Running Zara scraper and upload...')
      processZaraCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'lulus':
      console.log('🏪 Running Lulus scraper and upload...')
      processLulusCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break
    case 'lululemon':
      console.log('🏪 Running LuluLemon scraper and upload...')
      processLululemonCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'freepeople':
      console.log('🏪 Running FreePeople scraper and upload...')
      processFreePeopleCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'gap':
      console.log('🏪 Running Gap scraper and upload...')
      processGapCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'nike':
      console.log('🏪 Running Nike scraper and upload...')
      processNikeCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'adidas':
      console.log('🏪 Running Adidas scraper and upload...')
      processAdidasCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'hm':
      console.log('🏪 Running H&M scraper and upload...')
      processHMCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'abercrombie':
      console.log('🏪 Running Abercrombie & Fitch scraper and upload...')
      processAbercrombieAndFitchCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'aritzia':
      console.log('🏪 Running Aritzia scraper and upload...')
      processAritziaCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'jcrew':
      console.log('🏪 Running J.Crew scraper and upload...')
      processJCrewCatalog()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    case 'upload-catalog-lulu':
      console.log('📤 Uploading existing catalog.jsonl.gz file...')
      uploadExistingCatalogFile()
        .then(() => process.exit(0))
        .catch((error) => {
          console.error('❌ Process failed:', error.message)
          process.exit(1)
        })
      break

    default:
      console.log('📖 Usage:')
      console.log(
        '  node upload-catalogs.js crawl              # Crawl all stores and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl            # Recrawl Shopify stores and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl-everlane   # Recrawl Everlane store and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl-good-american  # Recrawl Good American store and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl-freepeople  # Recrawl FreePeople store and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl-lulus      # Recrawl Lulus store and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl-aritzia    # Recrawl Aritzia store and upload'
      )
      console.log(
        '  node upload-catalogs.js recrawl-lululemon  # Recrawl Lululemon store and upload'
      )
      console.log(
        '  node upload-catalogs.js store <url> [country]  # Process single store'
      )
      console.log(
        '  node upload-catalogs.js test               # Test SFTP connection'
      )
      console.log(
        '  node upload-catalogs.js custom             # Process all custom crawlers'
      )
      console.log(
        '  node upload-catalogs.js everlane           # Process Everlane catalog'
      )
      console.log(
        '  node upload-catalogs.js good-american      # Process Good American catalog'
      )
      console.log(
        '  node upload-catalogs.js walmart <category-id> [max-pages]  # Process Walmart catalog'
      )
      console.log(
        '  node upload-catalogs.js myntra                              # Process Myntra catalog'
      )
      console.log(
        "  node upload-catalogs.js zara                                # Process Zara catalog (Men's & Women's)"
      )
      console.log(
        '  node upload-catalogs.js lulus                              # Process Lulus catalog'
      )
      console.log(
        '  node upload-catalogs.js lululemon                          # Process Lululemon catalog'
      )
      console.log(
        '  node upload-catalogs.js freepeople                         # Process FreePeople catalog'
      )
      console.log(
        '  node upload-catalogs.js gap                                # Process Gap catalog'
      )
      console.log(
        '  node upload-catalogs.js nike                               # Process Nike catalog'
      )
      console.log(
        '  node upload-catalogs.js adidas                             # Process Adidas catalog'
      )
      console.log(
        '  node upload-catalogs.js hm                                 # Process H&M catalog'
      )
      console.log(
        '  node upload-catalogs.js aritzia                            # Process Aritzia catalog'
      )
      console.log(
        '  node upload-catalogs.js jcrew                              # Process J.Crew catalog'
      )
      console.log(
        '  node upload-catalogs.js upload-catalog-lulu                # Upload existing catalog.jsonl.gz file'
      )
      console.log('')
      console.log('📝 Examples:')
      console.log('  node upload-catalogs.js store https://store.com US')
      console.log('  node upload-catalogs.js walmart 5438 2')
      console.log('  node upload-catalogs.js zara')
      console.log('  node upload-catalogs.js lulus')
      console.log('  node upload-catalogs.js lululemon')
      console.log('  node upload-catalogs.js freepeople')
      console.log('  node upload-catalogs.js gap')
      console.log('  node upload-catalogs.js nike')
      console.log('  node upload-catalogs.js adidas')
      console.log('  node upload-catalogs.js hm')
      console.log('  node upload-catalogs.js aritzia')
      console.log('  node upload-catalogs.js jcrew')
      console.log('  node upload-catalogs.js upload-catalog-lulu')
      break
  }
}
