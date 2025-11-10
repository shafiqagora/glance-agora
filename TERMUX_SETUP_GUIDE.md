<!-- @format -->

# Neiman Marcus Crawler - Android Termux Setup Guide

## Why Termux?

Android Termux can help bypass 403 errors because:

- Mobile IPs are often less blocked than datacenter IPs
- Mobile user agents are more trusted
- Different network fingerprinting
- Can use mobile data or WiFi

## Step-by-Step Setup

### Step 1: Install Termux

**Option A: From F-Droid (Recommended - Latest Version)**

1. Open browser on Android
2. Go to: https://f-droid.org/en/packages/com.termux/
3. Download F-Droid APK (if not installed)
4. Install F-Droid
5. Open F-Droid and search "Termux"
6. Install Termux

**Option B: From GitHub (Latest Release)**

1. Go to: https://github.com/termux/termux-app/releases
2. Download the latest `termux-app_v*.apk`
3. Install the APK (enable "Install from Unknown Sources" if needed)

### Step 2: Initial Termux Setup

1. Open Termux app
2. Update packages:

```bash
pkg update && pkg upgrade
```

3. Install essential tools:

```bash
pkg install -y git nodejs wget curl
```

4. Verify Node.js installation:

```bash
node --version
npm --version
```

### Step 3: Transfer Crawler Files to Termux

**Option A: Using Git (Recommended)**

```bash
# Clone your repository
cd ~
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO
```

**Option B: Using Termux File Manager**

1. Install Termux file manager:

```bash
pkg install termux-api
termux-setup-storage
```

2. Copy files from Downloads:

```bash
# Grant storage permission when prompted
cp ~/storage/downloads/neimanmarcus-crawler.js ~/
cp ~/storage/downloads/neimanmarcus-console.js ~/
mkdir -p ~/crawler
mv ~/neimanmarcus-*.js ~/crawler/
cd ~/crawler
```

**Option C: Manual Copy-Paste**

1. Create the crawler file:

```bash
mkdir -p ~/crawler
cd ~/crawler
nano neimanmarcus-crawler.js
```

2. Copy the entire content from your computer and paste into nano
3. Save: `Ctrl+X`, then `Y`, then `Enter`

### Step 4: Install Dependencies

```bash
cd ~/crawler

# Create package.json if it doesn't exist
cat > package.json << 'EOF'
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
EOF

# Install dependencies
npm install
```

### Step 5: Copy Helper Files

If you have `utils/helper.js`, copy it:

```bash
mkdir -p ~/crawler/utils
# Copy helper.js content or clone full repo
```

### Step 6: Modify for Mobile User Agent

Create a mobile-optimized version:

```bash
nano ~/crawler/neimanmarcus-crawler-mobile.js
```

Use mobile user agents in the HEADERS section:

```javascript
const HEADERS = {
  "User-Agent":
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
  Accept: "application/json, text/plain, */*",
  "Accept-Language": "en-US,en;q=0.9",
  "Accept-Encoding": "gzip, deflate, br",
  Referer: "https://www.neimanmarcus.com/",
  Origin: "https://www.neimanmarcus.com",
  Connection: "keep-alive",
  "Sec-Fetch-Dest": "empty",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Site": "same-origin",
};
```

### Step 7: Run the Crawler

**Without Proxy (Using Mobile Data/WiFi):**

```bash
cd ~/crawler
node neimanmarcus-crawler.js
```

**With Proxy (if needed):**

```bash
# Set proxy environment variables
export PROXY_USERNAME="your_username"
export PROXY_PASSWORD="your_password"
export PROXY_PORT="10000"

node neimanmarcus-crawler.js
```

### Step 8: Monitor Progress

Termux will show all console output. To keep it running in background:

```bash
# Run in background
nohup node neimanmarcus-crawler.js > crawler.log 2>&1 &

# View logs
tail -f crawler.log

# Check if still running
ps aux | grep node
```

### Step 9: Access Output Files

```bash
# List output files
ls -lh ~/crawler/output/US/neimanmarcus-US/

# View a file
cat ~/crawler/output/US/neimanmarcus-US/catalog-chunk-*.json | head -50

# Copy to Downloads folder
cp ~/crawler/output/US/neimanmarcus-US/*.json ~/storage/downloads/
```

## Troubleshooting

### Issue: Permission Denied

```bash
chmod +x neimanmarcus-crawler.js
```

### Issue: Node modules not found

```bash
npm install --force
```

### Issue: Network timeout

- Try switching between WiFi and Mobile Data
- Use a VPN app on Android
- Check if you're behind a firewall

### Issue: Storage permission

```bash
termux-setup-storage
# Grant permission when prompted
```

## Tips for Better Success

1. **Use Mobile Data**: Mobile IPs are often less blocked
2. **Switch Networks**: Try different WiFi networks or mobile data
3. **Use VPN**: Install a VPN app on Android for IP rotation
4. **Run During Off-Peak Hours**: Less traffic = less blocking
5. **Add Delays**: Increase delays between requests in the script

## Quick Start Script

Create a startup script:

```bash
nano ~/crawler/start.sh
```

Add:

```bash
#!/data/data/com.termux/files/usr/bin/bash
cd ~/crawler
echo "🚀 Starting Neiman Marcus Crawler..."
echo "📱 Running on Android Termux"
node neimanmarcus-crawler.js
```

Make executable:

```bash
chmod +x ~/crawler/start.sh
```

Run:

```bash
~/crawler/start.sh
```

## Alternative: Use Termux Widget

1. Install Termux Widget:

```bash
pkg install termux-api
```

2. Create widget script:

```bash
mkdir -p ~/.shortcuts
cat > ~/.shortcuts/crawler.sh << 'EOF'
#!/data/data/com.termux/files/usr/bin/bash
cd ~/crawler && node neimanmarcus-crawler.js
EOF
chmod +x ~/.shortcuts/crawler.sh
```

3. Add Termux Widget to home screen
4. Tap widget to run crawler

## Monitoring on Computer

If you want to monitor from your computer:

1. Install SSH in Termux:

```bash
pkg install openssh
sshd
```

2. Get IP address:

```bash
ifconfig
```

3. Connect from computer:

```bash
ssh -p 8022 YOUR_ANDROID_IP
```

## Success Indicators

✅ No 403 errors in output
✅ Products being fetched successfully
✅ JSON files being created
✅ No "BLOCKED" messages

Good luck! 🚀
