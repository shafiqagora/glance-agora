#!/data/data/com.termux/files/usr/bin/bash
# Quick Setup Script for Neiman Marcus Crawler on Termux
# Copy and paste this entire script into Termux

echo "🚀 Neiman Marcus Crawler - Termux Setup"
echo "========================================"
echo ""

# Update packages
echo "📦 Updating packages..."
pkg update -y && pkg upgrade -y

# Install dependencies
echo "📦 Installing Node.js and dependencies..."
pkg install -y nodejs git wget curl

# Verify installation
echo "✅ Checking versions..."
node --version
npm --version

# Create crawler directory
echo "📁 Creating crawler directory..."
mkdir -p ~/crawler
cd ~/crawler

# Create package.json
echo "📝 Creating package.json..."
cat > package.json << 'PKGEOF'
{
  "name": "neimanmarcus-crawler",
  "version": "1.0.0",
  "description": "Neiman Marcus crawler for Android Termux",
  "main": "neimanmarcus-crawler.js",
  "scripts": {
    "start": "node neimanmarcus-crawler.js"
  },
  "dependencies": {
    "axios": "^1.6.0",
    "https-proxy-agent": "^7.0.2"
  }
}
PKGEOF

# Install npm packages
echo "📦 Installing npm packages..."
npm install

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Copy neimanmarcus-crawler.js to ~/crawler/"
echo "2. Copy utils/helper.js to ~/crawler/utils/ (if using proxy)"
echo "3. Run: cd ~/crawler && node neimanmarcus-crawler.js"
echo ""
echo "💡 Tip: Use 'termux-setup-storage' to access Downloads folder"

