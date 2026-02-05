#!/bin/sh
set -e

# Configuration
REPO="derekwisong/datui"
BINARY_NAME="datui"
MANPAGE_NAME="${BINARY_NAME}.1"
MANPAGE_GZ_NAME="${MANPAGE_NAME}.gz"
GITHUB_URL="https://github.com/$REPO/releases/latest/download"

# Does the user want to assume yes?
for arg in "$@"; do
  if [ "$arg" = "-y" ] || [ "$arg" = "--yes" ]; then
    ASSUME_YES=true
    break
  fi
done

# When piped (e.g. curl ... | sh), stdin is not a terminal; use -y to avoid
# apt/dnf prompting and aborting the installation.
if [ ! -t 0 ] || [ "$ASSUME_YES" = true ]; then
    NONINTERACTIVE="-y"
else
    NONINTERACTIVE=""
fi

# Identify System
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Normalize Architecture
case "$ARCH" in
    x86_64)  CANONICAL_ARCH="amd64" ;;
    aarch64|arm64) CANONICAL_ARCH="arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# if -y was passed, skip the Arch Linux prompt
# If Arch user, prompt to install via an AUR helper instead
if [ -f /etc/arch-release ] && [ "$ASSUME_YES" != true ]; then
    echo "-------------------------------------------------------"
    echo " ARCH LINUX DETECTED"
    echo "-------------------------------------------------------"
    echo "You may install via the AUR instead if desired:"
    echo "  paru -S datui-bin  (or your preferred AUR helper)"
    echo ""
    
    # Prompt for confirmation
    printf "Would you like to continue with the direct binary install anyway? [y/N]: "
    read -r response < /dev/tty
    case "$response" in
        [yY][eE][sS]|[yY]) 
            echo "Proceeding with binary installation..."
            ;;
        *)
            echo "Installation cancelled. Please use the AUR package."
            exit 0
            ;;
    esac
fi

# Determine latest version tag from GitHub
TAG=$(curl -sI https://github.com/$REPO/releases/latest | grep -i "location:" | awk -F/ '{print $NF}' | tr -d '\r\n')
# Trim the 'v' for the package filenames (e.g., 0.2.0)
VERSION="${TAG#v}"

echo "--- Installing $BINARY_NAME $VERSION for $OS ($CANONICAL_ARCH) ---"

# Detect Package Manager / Format
case "$OS" in
    linux*)
        if [ -f /etc/debian_version ]; then
            # on ubuntu/debian-based systems use the deb package
            FORMAT="deb"
            FILENAME="${BINARY_NAME}_${VERSION}-1_${CANONICAL_ARCH}.deb"
        elif [ -f /etc/redhat-release ] || [ -f /etc/fedora-release ]; then
            # on redhat/rpm-based systems use the rpm package
            FORMAT="rpm"
            FILENAME="${BINARY_NAME}-${VERSION}-1.${ARCH}.rpm"
        else
            # on other systems use the binary tarball
            FORMAT="tar.gz"
            FILENAME="${BINARY_NAME}-${VERSION}-${ARCH}.tar.gz"
        fi
        ;;
    darwin*)
        # macOS: release tarballs use target triple (datui-v0.2.32-aarch64-apple-darwin.tar.gz)
        FORMAT="tar.gz"
        case "$ARCH" in
            arm64|aarch64) MACOS_TARGET="aarch64-apple-darwin" ;;
            x86_64)        MACOS_TARGET="x86_64-apple-darwin" ;;
            *) echo "Unsupported macOS architecture: $ARCH"; exit 1 ;;
        esac
        FILENAME="${BINARY_NAME}-${TAG}-${MACOS_TARGET}.tar.gz"
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

# Download
TMP_DIR=$(mktemp -d)
echo "Downloading $FILENAME... ($TMP_DIR)"
curl -sSL "$GITHUB_URL/$FILENAME" -o "$TMP_DIR/$FILENAME"

# Install based on format
case "$FORMAT" in
    deb)
        echo "Installing via apt..."
        sudo apt-get update -qq && sudo apt-get install $NONINTERACTIVE "$TMP_DIR/$FILENAME"
        ;;
    rpm)
        echo "Installing via dnf..."
        sudo dnf install $NONINTERACTIVE "$TMP_DIR/$FILENAME"
        ;;
    tar.gz)
        echo "Extracting binary to /usr/local/bin..."
        tar -xzf "$TMP_DIR/$FILENAME" -C "$TMP_DIR"

        if [ -f "$TMP_DIR/$MANPAGE_NAME" ]; then
            # macOS tarball has datui/datui.1 at root
            MANPAGE_PATH="$TMP_DIR/$MANPAGE_NAME"
        else
            # Linux tarball has target/release/datui.1.gz
            MANPAGE_PATH="$TMP_DIR/target/release/$MANPAGE_GZ_NAME"
        fi

        sudo install -d /usr/local/bin
        sudo install -m 755 "$TMP_DIR/$BINARY_NAME" "/usr/local/bin/$BINARY_NAME"
        sudo install -d /usr/local/share/man/man1
        sudo install -m 644 "$MANPAGE_PATH" "/usr/local/share/man/man1/"
        ;;
esac

# Cleanup
rm -rf "$TMP_DIR"
echo "--- $BINARY_NAME installed successfully! ---"
$BINARY_NAME --version
