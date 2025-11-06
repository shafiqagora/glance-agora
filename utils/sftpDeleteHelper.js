const SFTPHelper = require('./sftpHelper')
const path = require('path')

class SFTPDeleteHelper extends SFTPHelper {
  /**
   * Recursively delete all files and folders in a directory
   * @param {string} remotePath - Remote directory path to delete
   */
  async deleteDirectoryRecursively(remotePath) {
    try {
      console.log(`🗑️  Starting recursive deletion of: ${remotePath}`)

      const exists = await this.sftp.exists(remotePath)
      if (!exists) {
        console.log(`⚠️  Directory does not exist: ${remotePath}`)
        return false
      }

      // Try to get stats, but if it fails, assume it's a directory if we can list it
      let isDirectory = false
      // If stat fails, try to list the directory to verify it's a directory
      try {
        await this.sftp.list(remotePath)
        isDirectory = true
        console.log(`✅ Confirmed ${remotePath} is a directory (by listing)`)
      } catch (listError) {
        console.log(`❌ Cannot list ${remotePath}, it may not be a directory`)
        isDirectory = false
      }

      if (!isDirectory) {
        console.log(`⚠️  Path is not a directory: ${remotePath}`)
        return false
      }

      await this._deleteDirectoryContents(remotePath)

      // Delete the empty directory itself
      await this.sftp.rmdir(remotePath)
      console.log(`✅ Successfully deleted directory: ${remotePath}`)

      return true
    } catch (error) {
      console.error(`❌ Error deleting directory ${remotePath}:`, error.message)
      throw error
    }
  }

  /**
   * Recursively delete contents of a directory
   * @param {string} remotePath - Remote directory path
   */
  async _deleteDirectoryContents(remotePath) {
    try {
      const items = await this.sftp.list(remotePath)

      for (const item of items) {
        const itemPath = path.posix.join(remotePath, item.name)

        if (item.type === 'd') {
          // It's a directory, recursively delete its contents first
          await this._deleteDirectoryContents(itemPath)
          await this.sftp.rmdir(itemPath)
          console.log(`🗂️  Deleted directory: ${itemPath}`)
        } else {
          // It's a file, delete it directly
          await this.sftp.delete(itemPath)
          console.log(`📄 Deleted file: ${itemPath}`)
        }
      }
    } catch (error) {
      console.error(
        `❌ Error deleting contents of ${remotePath}:`,
        error.message
      )
      throw error
    }
  }

  /**
   * Delete the 24-JUN-2025 folder and all its contents
   */
  async deleteJune24Folder() {
    try {
      console.log('🚀 Starting deletion of 24-JUN-2025 folder...')

      if (!this.isConnected) {
        await this.connect()
      }

      const targetFolder = '24-JUN-2025'
      const success = await this.deleteDirectoryRecursively(targetFolder)

      if (success) {
        console.log(
          '✅ Successfully deleted 24-JUN-2025 folder and all contents'
        )
      } else {
        console.log('⚠️  Deletion completed with warnings')
      }

      return success
    } catch (error) {
      console.error('❌ Failed to delete 24-JUN-2025 folder:', error.message)
      throw error
    }
  }

  /**
   * List contents of a directory before deletion (for safety)
   * @param {string} remotePath - Remote directory path
   */
  async listDirectoryContents(remotePath) {
    try {
      console.log(`📋 Listing contents of: ${remotePath}`)

      const exists = await this.sftp.exists(remotePath)
      if (!exists) {
        console.log(`⚠️  Directory does not exist: ${remotePath}`)
        return []
      }

      const items = await this.sftp.list(remotePath)
      console.log(`Found ${items.length} items:`)

      items.forEach((item) => {
        const type = item.type === 'd' ? '📁' : '📄'
        const size =
          item.type === 'd' ? '' : ` (${this.formatFileSize(item.size)})`
        console.log(`  ${type} ${item.name}${size}`)
      })

      return items
    } catch (error) {
      console.error(`❌ Error listing directory ${remotePath}:`, error.message)
      throw error
    }
  }

  /**
   * Format file size in human readable format
   * @param {number} bytes - File size in bytes
   */
  formatFileSize(bytes) {
    if (bytes === 0) return '0 B'
    const k = 1024
    const sizes = ['B', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }
}

module.exports = SFTPDeleteHelper
