#!/usr/bin/env bash
set -euo pipefail

# Load commit pin
set -a
source .env
set +a

: "${TRADING_FRAMEWORK_COMMIT:?Missing TRADING_FRAMEWORK_COMMIT in .env}"

echo "🔧 Compiling requirements with pip-tools..."
echo "📌 Pinning trading-framework at commit: $TRADING_FRAMEWORK_COMMIT"

python -m pip install --upgrade \
  "pip>=23.3,<25" \
  "setuptools>=68,<81" \
  "wheel>=0.41,<1" \
  "pip-tools>=7.3,<7.6"

# Temporary requirements input for git dependency
cat > _git_deps.in <<EOF
trading-framework @ git+https://github.com/trading-engineering/trading-framework.git@$TRADING_FRAMEWORK_COMMIT
EOF

# Compile runtime deps + git pin
python -m piptools compile pyproject.toml _git_deps.in -o requirements.txt

# Compile dev deps
python -m piptools compile pyproject.toml _git_deps.in --extra dev -o requirements-dev.txt

rm _git_deps.in

echo "✅ requirements.txt and requirements-dev.txt updated"
