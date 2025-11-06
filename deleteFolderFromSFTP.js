#!/usr/bin/env node
require('dotenv').config()
const SFTPDeleteHelper = require('./utils/sftpDeleteHelper')
const readline = require('readline')

// Create readline interface for user input
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
})

// Promise wrapper for readline
function question(prompt) {
  return new Promise((resolve) => {
    rl.question(prompt, resolve)
  })
}

async function main() {
  console.log('🗑️  SFTP Directory Deletion Script')
  console.log('=====================================\n')

  const sftpHelper = new SFTPDeleteHelper()
  const targetFolder = '24-JUN-2025'

  try {
    // Connect to SFTP server
    console.log('📡 Connecting to SFTP server...')
    await sftpHelper.connect()

    // List contents before deletion
    console.log('\n📋 Checking contents of target folder...')
    await sftpHelper.listDirectoryContents(targetFolder)

    // Safety confirmation
    console.log(
      '\n⚠️  WARNING: This will permanently delete ALL files and folders in the 24-JUN-2025 directory!'
    )
    console.log('   This action cannot be undone.\n')

    const confirm = await question(
      'Are you sure you want to proceed? (yes/no): '
    )

    if (confirm.toLowerCase() !== 'yes') {
      console.log('❌ Deletion cancelled by user.')
      process.exit(0)
    }

    // Double confirmation for safety
    const doubleConfirm = await question('Type "DELETE" to confirm deletion: ')

    if (doubleConfirm !== 'DELETE') {
      console.log('❌ Deletion cancelled - incorrect confirmation.')
      process.exit(0)
    }

    console.log('\n🚀 Proceeding with deletion...\n')

    // Perform the deletion
    const success = await sftpHelper.deleteJune24Folder()

    if (success) {
      console.log('\n✅ Deletion completed successfully!')
      console.log(
        '   The 24-JUN-2025 folder and all its contents have been removed.'
      )
    } else {
      console.log('\n⚠️  Deletion completed with warnings.')
    }
  } catch (error) {
    console.error('\n❌ Error during deletion process:', error.message)
    process.exit(1)
  } finally {
    await sftpHelper.disconnect()
    rl.close()
  }
}

// Handle process termination
process.on('SIGINT', () => {
  console.log('\n\n❌ Process interrupted by user.')
  rl.close()
  process.exit(0)
})

process.on('SIGTERM', () => {
  console.log('\n\n❌ Process terminated.')
  rl.close()
  process.exit(0)
})

// Run the script
if (require.main === module) {
  main().catch((error) => {
    console.error('❌ Unhandled error:', error)
    process.exit(1)
  })
}

module.exports = main
