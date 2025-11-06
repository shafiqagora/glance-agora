const { connectDB, disconnectDB } = require('./database/connection')
const Product = require('./models/Product')
const Store = require('./models/Store')

// Brands to delete
const brandsToDelete = [
  {
    brandName: 'Zara',
    url: 'zara.com',
  },
]

async function deleteStoresAndProducts() {
  try {
    console.log('🔌 Connecting to MongoDB...')
    await connectDB()
    console.log('✅ Connected to MongoDB')

    for (const brand of brandsToDelete) {
      console.log(`\n🔍 Processing brand: ${brand.brandName} (${brand.url})`)

      // Find stores that match the brand name or URL
      const stores = await Store.find({
        $or: [
          { name: { $regex: brand.brandName, $options: 'i' } },
          { storeUrl: { $regex: brand.url, $options: 'i' } },
        ],
      })

      if (stores.length === 0) {
        console.log(`❌ No stores found for ${brand.brandName}`)
        continue
      }

      console.log(`📦 Found ${stores.length} store(s) for ${brand.brandName}`)

      for (const store of stores) {
        console.log(`\n🏪 Processing store: ${store.name} (${store.storeUrl})`)

        // Get all product IDs from this store
        const productIds = store.products || []
        console.log(`📦 Store has ${productIds.length} products`)

        if (productIds.length > 0) {
          // Delete all products from the products collection
          const deleteResult = await Product.deleteMany({
            _id: { $in: productIds },
          })
          console.log(
            `🗑️  Deleted ${deleteResult.deletedCount} products from products collection`
          )
        }

        // Also delete products by retailer_domain as a backup
        const domainDeleteResult = await Product.deleteMany({
          retailer_domain: { $regex: brand.url, $options: 'i' },
        })
        console.log(
          `🗑️  Deleted ${domainDeleteResult.deletedCount} additional products by domain`
        )

        // Delete the store itself
        const storeDeleteResult = await Store.deleteOne({ _id: store._id })
        if (storeDeleteResult.deletedCount > 0) {
          console.log(`🗑️  Successfully deleted store: ${store.name}`)
        } else {
          console.log(`❌ Failed to delete store: ${store.name}`)
        }
      }
    }

    console.log('\n✅ Deletion process completed!')
  } catch (error) {
    console.error('❌ Error during deletion process:', error.message)
    throw error
  } finally {
    console.log('🔌 Disconnecting from MongoDB...')
    await disconnectDB()
    console.log('✅ Disconnected from MongoDB')
  }
}

// Run the script
if (require.main === module) {
  deleteStoresAndProducts()
    .then(() => {
      console.log('🎉 Script completed successfully!')
      process.exit(0)
    })
    .catch((error) => {
      console.error('💥 Script failed:', error.message)
      process.exit(1)
    })
}

module.exports = { deleteStoresAndProducts }
