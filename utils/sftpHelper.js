const Client = require('ssh2-sftp-client')
const fs = require('fs')
const path = require('path')
require('dotenv').config()

class SFTPHelper {
  constructor() {
    this.sftp = new Client()
    this.isConnected = false

    // SFTP configuration from environment variables
    this.config = {
      host: process.env.SFTP_HOST,
      port: process.env.SFTP_PORT || 22,
      username: process.env.SFTP_USERNAME,
      privateKey: process.env.SFTP_PRIVATE_KEY_PATH
        ? fs.readFileSync(process.env.SFTP_PRIVATE_KEY_PATH)
        : null,
      readyTimeout: 99999,
      retries: 3,
      retry_factor: 2,
      retry_minTimeout: 2000,
    }
  }

  /**
   * Connect to SFTP server
   */
  async connect() {
    try {
      if (this.isConnected) {
        console.log('📡 Already connected to SFTP server')
        return true
      }

      // Validate configuration
      if (
        !this.config.host ||
        !this.config.username ||
        !this.config.privateKey
      ) {
        throw new Error(
          'Missing SFTP configuration. Please check SFTP_HOST, SFTP_USERNAME, and SFTP_PRIVATE_KEY_PATH environment variables.'
        )
      }

      console.log(
        `📡 Connecting to SFTP server: ${this.config.username}@${this.config.host}:${this.config.port}`
      )

      await this.sftp.connect(this.config)
      this.isConnected = true

      console.log('✅ Connected to SFTP server successfully')
      return true
    } catch (error) {
      console.error('❌ Failed to connect to SFTP server:', error.message)
      throw error
    }
  }

  /**
   * Disconnect from SFTP server
   */
  async disconnect() {
    try {
      if (this.isConnected) {
        await this.sftp.end()
        this.isConnected = false
        console.log('🔌 Disconnected from SFTP server')
      }
    } catch (error) {
      console.error('❌ Error disconnecting from SFTP server:', error.message)
    }
  }

  /**
   * Create directory structure on remote server
   * @param {string} remotePath - Remote directory path (relative to SFTP root)
   */
  async ensureDirectoryExists(remotePath) {
    try {
      const exists = await this.sftp.exists(remotePath)

      if (!exists) {
        // Determine what type of directory we're creating
        const pathParts = remotePath.split('/').filter(Boolean)
        const isCountryCodeDir = pathParts.length === 1
        const isStoreDir = pathParts.length === 2

        if (isCountryCodeDir) {
          console.log(`📁 Creating country directory: ${pathParts[0]}/`)
        } else if (isStoreDir) {
          console.log(
            `📁 Creating store directory: ${pathParts[0]}/${pathParts[1]}/`
          )
        } else {
          console.log(`📁 Creating directory: ${remotePath}`)
        }

        // Create the directory structure recursively
        await this.sftp.mkdir(remotePath, true)

        // Set permissions on the newly created directory
        try {
          await this.sftp.chmod(remotePath, '755')
          console.log(`✅ Directory created with permissions: ${remotePath}`)
        } catch (chmodError) {
          console.log(
            `✅ Directory created: ${remotePath} (could not set permissions)`
          )
        }

        return true
      } else {
        // Only log for store directories to avoid spam
        const pathParts = remotePath.split('/').filter(Boolean)
        const isStoreDir = pathParts.length === 2

        if (isStoreDir) {
          console.log(
            `📁 Store directory already exists: ${pathParts[0]}/${pathParts[1]}/`
          )
        }
        return false
      }
    } catch (error) {
      console.error(`❌ Error creating directory ${remotePath}:`, error.message)
      throw error
    }
  }

  /**
   * Upload a single file to SFTP server
   * @param {string} localFilePath - Local file path
   * @param {string} remoteFilePath - Remote file path
   */
  async uploadFile(localFilePath, remoteFilePath) {
    try {
      if (!fs.existsSync(localFilePath)) {
        throw new Error(`Local file does not exist: ${localFilePath}`)
      }

      // Ensure remote directory exists
      const remoteDir = path.dirname(remoteFilePath)
      await this.ensureDirectoryExists(remoteDir)

      console.log(`📤 Uploading: ${localFilePath} → ${remoteFilePath}`)

      const fileStats = fs.statSync(localFilePath)
      const fileSizeMB = (fileStats.size / (1024 * 1024)).toFixed(2)

      const startTime = Date.now()
      await this.sftp.fastPut(localFilePath, remoteFilePath)
      const uploadTime = ((Date.now() - startTime) / 1000).toFixed(2)

      console.log(
        `✅ Upload completed: ${path.basename(
          localFilePath
        )} (${fileSizeMB}MB in ${uploadTime}s)`
      )
      return true
    } catch (error) {
      console.error(`❌ Failed to upload ${localFilePath}:`, error.message)
      throw error
    }
  }

  /**
   * Upload store catalog file (catalog.jsonl.gz only)
   * @param {Object} storeInfo - Store information
   * @param {Object} filePaths - Object containing file paths (must include jsonPath)
   */
  async uploadStoreCatalog(storeInfo, filePaths) {
    try {
      if (!this.isConnected) {
        await this.connect()
      }

      const { brandName, url, region, countryCode } = storeInfo
      const storeDomain = this.extractDomain(url)

      // Format current date as DD-MMM-YYYY (e.g., 09-JUN-2025)
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
      const dateString = `${day}-${month}-${year}`

      // Create remote directory structure: {date}/{countryCode}/{retailername-countryCode}/
      const cleanBrandName = brandName.replace(/[^a-zA-Z0-9.-]/g, '-')
      const storeRemotePath = path.posix.join(
        dateString,
        countryCode || 'US',
        `${cleanBrandName}-${countryCode || 'US'}`
      )

      console.log(`\n🏪 Uploading catalog for: ${brandName}`)
      console.log(`📍 Remote path: ${storeRemotePath}`)

      // Upload only catalog.jsonl.gz file
      if (!filePaths.jsonPath || !fs.existsSync(filePaths.jsonPath)) {
        throw new Error(
          `Catalog file (jsonPath) not found or does not exist: ${filePaths.jsonPath}`
        )
      }

      const remoteJsonPath = path.posix.join(
        storeRemotePath,
        path.basename(filePaths.jsonPath)
      )

      // Upload the catalog file
      await this.uploadFile(filePaths.jsonPath, remoteJsonPath)

      console.log(`✅ Successfully uploaded catalog for ${brandName}`)
      return {
        success: true,
        storeDomain,
        remotePath: storeRemotePath,
        filesUploaded: 1,
        uploadedFile: path.basename(filePaths.jsonPath),
      }
    } catch (error) {
      console.error(
        `❌ Failed to upload catalog for ${storeInfo.brandName}:`,
        error.message
      )
      throw error
    }
  }

  /**
   * Upload multiple store catalogs
   * @param {Array} stores - Array of store information with file paths
   */
  async uploadMultipleStores(stores) {
    const results = {
      successful: [],
      failed: [],
      total: stores.length,
    }

    try {
      // Validate input
      if (!Array.isArray(stores) || stores.length === 0) {
        throw new Error('No stores provided for upload')
      }

      await this.connect()

      console.log(`🚀 Starting upload of ${stores.length} store catalogs...`)

      for (let i = 0; i < stores.length; i++) {
        const store = stores[i]

        try {
          // Validate store object
          if (!store || !store.brandName) {
            throw new Error('Invalid store object: missing brandName')
          }

          console.log(
            `\n[${i + 1}/${stores.length}] Uploading: ${store.brandName}`
          )

          const uploadResult = await this.uploadStoreCatalog(
            store,
            store.filePaths || {}
          )

          results.successful.push({
            ...store,
            upload_result: uploadResult,
          })

          console.log(`✅ Successfully uploaded: ${store.brandName}`)
        } catch (error) {
          const errorMessage = error.message || 'Unknown error occurred'
          console.error(
            `❌ Failed to upload ${store.brandName || 'Unknown store'}:`,
            errorMessage
          )

          results.failed.push({
            ...store,
            error: errorMessage,
            timestamp: new Date().toISOString(),
          })

          // Continue to next store - don't let one failure stop the entire process
          console.log(`⏭️  Continuing to next store...`)
        }

        // Add small delay between uploads to be respectful to the server
        if (i < stores.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, 1000))
        }
      }

      // Enhanced summary with more details
      console.log(`\n📊 Upload Summary:`)
      console.log(`  Total stores: ${results.total}`)
      console.log(`  Successful: ${results.successful.length}`)
      console.log(`  Failed: ${results.failed.length}`)

      if (results.failed.length > 0) {
        console.log(`\n❌ Failed stores:`)
        results.failed.forEach((failedStore, index) => {
          console.log(
            `  ${index + 1}. ${failedStore.brandName || 'Unknown'}: ${
              failedStore.error
            }`
          )
        })
      }

      if (results.successful.length > 0) {
        console.log(`\n✅ Successfully uploaded stores:`)
        results.successful.forEach((successStore, index) => {
          console.log(`  ${index + 1}. ${successStore.brandName}`)
        })
      }

      return results
    } catch (error) {
      console.error('❌ Critical error in uploadMultipleStores:', error.message)

      // Even if there's a critical error, return partial results if any stores were processed
      if (results.successful.length > 0 || results.failed.length > 0) {
        console.log(`⚠️  Returning partial results due to critical error`)
        return {
          ...results,
          criticalError: error.message,
        }
      }

      throw error
    } finally {
      // Always attempt to disconnect, even if there were errors
      try {
        await this.disconnect()
      } catch (disconnectError) {
        console.error('⚠️  Error during disconnect:', disconnectError.message)
      }
    }
  }

  /**
   * Extract domain from URL
   * @param {string} url - URL to extract domain from
   */
  extractDomain(url) {
    try {
      const parsedUrl = new URL(url)
      let domain = parsedUrl.hostname

      if (domain.startsWith('www.')) {
        domain = domain.substring(4)
      }

      return domain.replace(/[^a-zA-Z0-9.-]/g, '-')
    } catch (error) {
      // Fallback for invalid URLs
      return url.replace(/[^a-zA-Z0-9.-]/g, '-')
    }
  }

  /**
   * Test SFTP connection
   */
  async testConnection() {
    try {
      console.log('🧪 Testing SFTP connection...')
      await this.connect()

      // Test current directory (should be the default SFTP root)
      console.log('✅ Connection test successful!')

      // Try to list contents and verify write permissions
      try {
        const files = await this.sftp.list('.')
        console.log(`📁 Found ${files.length} items in current directory`)
        console.log(files)
        console.log(
          `ℹ️  Upload process will create subdirectories: {date}/{countryCode}/{storeDomain}/`
        )
      } catch (listError) {
        console.log(
          `⚠️  Cannot list current directory contents - check permissions`
        )
        await this.disconnect()
        return false
      }

      await this.disconnect()
      return true
    } catch (error) {
      console.error('❌ Connection test failed:', error.message)
      return false
    }
  }

  /**
   * Upload existing catalog.jsonl.gz file with connection management
   * @param {string} localFilePath - Local path to the catalog.jsonl.gz file
   * @param {string} remotePath - Remote path where to upload the file
   */
  async uploadExistingCatalogFile(localFilePath, remotePath) {
    try {
      // Connect to SFTP server
      await this.connect()

      // Check if local file exists
      if (!fs.existsSync(localFilePath)) {
        throw new Error(`Local file does not exist: ${localFilePath}`)
      }

      // Get file stats for logging
      const fileStats = fs.statSync(localFilePath)
      const fileSizeKB = (fileStats.size / 1024).toFixed(2)

      console.log(`📄 File size: ${fileSizeKB} KB`)
      console.log(`📤 Uploading to: ${remotePath}`)

      // Ensure remote directory exists
      const remoteDir = path.dirname(remotePath)
      await this.ensureDirectoryExists(remoteDir)

      // Upload the file
      const startTime = Date.now()
      await this.sftp.fastPut(localFilePath, remotePath)
      const uploadTime = ((Date.now() - startTime) / 1000).toFixed(2)

      console.log(
        `✅ Upload completed: ${path.basename(
          localFilePath
        )} (${fileSizeKB}KB in ${uploadTime}s)`
      )

      return {
        success: true,
        remotePath,
        fileSize: fileSizeKB,
        uploadTime: uploadTime,
      }
    } catch (error) {
      console.error(`❌ Failed to upload ${localFilePath}:`, error.message)
      return {
        success: false,
        error: error.message,
      }
    } finally {
      // Always disconnect
      await this.disconnect()
    }
  }
}

module.exports = SFTPHelper
