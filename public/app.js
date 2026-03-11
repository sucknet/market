const statusEl = document.getElementById('status');
const countEl = document.getElementById('count');
const updatedEl = document.getElementById('updated');
const gridEl = document.getElementById('grid');
const emptyEl = document.getElementById('empty');
const refreshBtn = document.getElementById('refreshBtn');
const loadMoreBtn = document.getElementById('loadMoreBtn');
const pageInfoEl = document.getElementById('pageInfo');
const nameFilterEl = document.getElementById('nameFilter');
const sortSelectEl = document.getElementById('sortSelect');
const walletStatusEl = document.getElementById('walletStatus');
const activeWalletSelectEl = document.getElementById('activeWalletSelect');
const privateKeyInputEl = document.getElementById('privateKeyInput');
const addWalletBtn = document.getElementById('addWalletBtn');
const walletListInfoEl = document.getElementById('walletListInfo');
const menuMarketBtn = document.getElementById('menuMarketBtn');
const menuMyAssetsBtn = document.getElementById('menuMyAssetsBtn');
const marketSection = document.getElementById('marketSection');
const myAssetsSection = document.getElementById('myAssetsSection');
const myAssetsGridEl = document.getElementById('myAssetsGrid');
const myAssetsEmptyEl = document.getElementById('myAssetsEmpty');
const myAssetsInfoEl = document.getElementById('myAssetsInfo');

let timer = null;
let refreshMs = 5000;
let currentPage = 1;
let pageSize = 60;
let totalListings = 0;
let hasMore = false;
let rendered = [];
let filterName = '';
let sortBy = 'newest';
let filterDebounce = null;
let rpcUrl = 'https://mainnet.fogo.io';
let defaultCollection = '';
let currentView = 'market';
let portfolioAssets = [];
let portfolioCollectionStats = {};
const POLL_MS = 5000;

const wallets = [];

function short(addr) {
  if (!addr || addr.length < 12) return addr || '-';
  return `${addr.slice(0, 4)}...${addr.slice(-4)}`;
}

function esc(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function deriveCollectionLabel(name, collection) {
  const text = String(name || '').trim();
  if (text) {
    const trimmed = text.replace(/\s*#\d+\s*$/i, '').trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return collection || 'unknown';
}

function decodeBase58(input) {
  const alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  const base = 58;
  const bytes = [0];
  for (let i = 0; i < input.length; i += 1) {
    const c = input[i];
    const value = alphabet.indexOf(c);
    if (value < 0) {
      throw new Error('Invalid base58 key');
    }
    let carry = value;
    for (let j = 0; j < bytes.length; j += 1) {
      carry += bytes[j] * base;
      bytes[j] = carry & 0xff;
      carry >>= 8;
    }
    while (carry > 0) {
      bytes.push(carry & 0xff);
      carry >>= 8;
    }
  }
  let leading = 0;
  for (let i = 0; i < input.length && input[i] === '1'; i += 1) {
    leading += 1;
  }
  const out = new Uint8Array(leading + bytes.length);
  for (let i = 0; i < bytes.length; i += 1) {
    out[out.length - 1 - i] = bytes[i];
  }
  return out;
}

function parsePrivateKey(raw) {
  const text = String(raw || '').trim();
  if (!text) {
    throw new Error('Private key is empty');
  }

  if (text.startsWith('[')) {
    const arr = JSON.parse(text);
    if (!Array.isArray(arr) || arr.length < 32) {
      throw new Error('Invalid secret key array');
    }
    return new Uint8Array(arr);
  }

  if (/^\d+(\s*,\s*\d+)+$/.test(text)) {
    return new Uint8Array(text.split(',').map((n) => Number(n.trim())));
  }

  return decodeBase58(text);
}

function getActiveWalletPubkey() {
  return activeWalletSelectEl.value || '';
}

function getWalletByPubkey(pubkey) {
  return wallets.find((w) => w.pubkey === pubkey) || null;
}

function updateWalletUi() {
  walletStatusEl.textContent = `Wallets: ${wallets.length} loaded`;
  walletListInfoEl.textContent = wallets.length
    ? wallets.map((w) => short(w.pubkey)).join(' | ')
    : 'No wallet added';

  const current = activeWalletSelectEl.value;
  const opts = ['<option value="">Active wallet for buy</option>'];
  for (const w of wallets) {
    opts.push(`<option value="${esc(w.pubkey)}">${esc(short(w.pubkey))}</option>`);
  }
  activeWalletSelectEl.innerHTML = opts.join('');

  if (current && wallets.some((w) => w.pubkey === current)) {
    activeWalletSelectEl.value = current;
  } else if (wallets.length > 0) {
    activeWalletSelectEl.value = wallets[0].pubkey;
  }
}

function setView(view) {
  currentView = view;
  const isMarket = view === 'market';
  marketSection.hidden = !isMarket;
  myAssetsSection.hidden = isMarket;
  menuMarketBtn.classList.toggle('active', isMarket);
  menuMyAssetsBtn.classList.toggle('active', !isMarket);
}

function toBufferFromBase64(base64) {
  const bin = atob(base64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i += 1) {
    bytes[i] = bin.charCodeAt(i);
  }
  return bytes;
}

async function buildTx(action, wallet, payload) {
  const res = await fetch('/api/tx/build', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, wallet, payload }),
  });
  const body = await res.json();
  if (!body.ok) {
    throw new Error(body.error || 'Build tx gagal');
  }
  return body;
}

async function signAndSendWithWallet(txBase64, wallet) {
  const conn = new window.solanaWeb3.Connection(rpcUrl, 'confirmed');
  const tx = window.solanaWeb3.Transaction.from(toBufferFromBase64(txBase64));
  tx.feePayer = new window.solanaWeb3.PublicKey(wallet.pubkey);
  tx.partialSign(wallet.keypair);

  const sig = await conn.sendRawTransaction(tx.serialize(), {
    skipPreflight: false,
    maxRetries: 3,
  });
  await conn.confirmTransaction(sig, 'confirmed');
  return sig;
}

async function executeTrade(action, walletPubkey, payload) {
  const wallet = getWalletByPubkey(walletPubkey);
  if (!wallet) {
    throw new Error('Wallet tidak ditemukan');
  }

  statusEl.textContent = `${action} tx building...`;
  const built = await buildTx(action, walletPubkey, payload);
  statusEl.textContent = `${action} signing...`;
  const sig = await signAndSendWithWallet(built.txBase64, wallet);
  statusEl.textContent = `${action} success: ${short(sig)}`;
}

function renderMarketCards(listings, mode = 'replace') {
  if (mode === 'replace') {
    gridEl.innerHTML = '';
  }
  emptyEl.hidden = listings.length > 0 || rendered.length > 0;

  const html = listings
    .map((item) => {
      const image = item.image || '';
      return `
        <article class="card">
          <img class="img" src="${esc(image)}" alt="${esc(item.name || 'NFT')}" loading="lazy" onerror="this.src='';this.alt='No image'" />
          <div class="body">
            <div class="name">${esc(item.name || item.assetMint)}</div>
            <div class="row">Mint: ${short(item.assetMint)}</div>
            <div class="row">Seller: ${short(item.seller)}</div>
            <div class="price">${item.priceUi} wFOGO</div>
            <div class="actions">
              <button type="button" class="buyBtn" data-listing="${esc(item.listingAddress)}">Buy</button>
              <a class="rebel-link" target="_blank" rel="noopener noreferrer" href="https://rebelorcs.com/asset/${esc(item.assetMint)}">Buy on RebelOrc</a>
            </div>
          </div>
        </article>
      `;
    })
    .join('');

  if (mode === 'replace') {
    gridEl.innerHTML = html;
  } else {
    gridEl.insertAdjacentHTML('beforeend', html);
  }
}

function renderPortfolioAssets(items, collectionStats = {}) {
  portfolioAssets = items || [];
  portfolioCollectionStats = collectionStats || {};
  myAssetsEmptyEl.hidden = portfolioAssets.length > 0;

  const byCollection = new Map();
  for (const item of portfolioAssets) {
    const collection = item.collectionLabel || deriveCollectionLabel(item.name, item.collection);
    if (!byCollection.has(collection)) {
      byCollection.set(collection, []);
    }
    byCollection.get(collection).push(item);
  }

  const collectionBlocks = [...byCollection.entries()]
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .map(([collection, itemsInCollection]) => {
      const stats = portfolioCollectionStats[collection] || {};
      const floor = stats.floorPriceUi;

      const byName = new Map();
      for (const item of itemsInCollection) {
        const nameKey = item.name || item.mint;
        if (!byName.has(nameKey)) {
          byName.set(nameKey, []);
        }
        byName.get(nameKey).push(item);
      }

      const groupedCards = [...byName.entries()]
        .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
        .map(([name, groupItems]) => {
          const cardsHtml = groupItems
            .map((item) => {
              const actionLabel = item.status === 'listed' ? 'Delist' : 'Listing';
              const actionClass = item.status === 'listed' ? 'assetDelistBtn' : 'assetListBtn';
              const payloadAttr = item.status === 'listed'
                ? `data-listing="${esc(item.listingAddress || '')}"`
                : `data-mint="${esc(item.mint)}" data-collection="${esc(item.collection || defaultCollection || '')}"`;
              const secondary = item.status === 'listed'
                ? `Listed at ${item.priceUi || '-'} wFOGO`
                : (item.inEscrow ? 'In escrow' : 'In wallet');
              return `
                <article class="card">
                  <img class="img" src="${esc(item.image || '')}" alt="${esc(item.name || item.mint)}" loading="lazy" onerror="this.src='';this.alt='No image'" />
                  <div class="body">
                    <div class="name">${esc(item.name || item.mint)}</div>
                    <div class="row">Wallet: ${short(item.owner)}</div>
                    <div class="row">Mint: ${short(item.mint)}</div>
                    <div class="row">${esc(secondary)}</div>
                    <div class="actions">
                      <button type="button" class="${actionClass}" data-owner="${esc(item.owner)}" ${payloadAttr}>${actionLabel}</button>
                    </div>
                  </div>
                </article>
              `;
            })
            .join('');

          return `
            <div class="asset-name-group">
              <div class="asset-name-title">${esc(name)} (${groupItems.length})</div>
              <div class="asset-card-grid">${cardsHtml}</div>
            </div>
          `;
        })
        .join('');

      return `
        <section class="asset-collection-block">
          <div class="asset-collection-head">
            <div>Collection: ${esc(collection)}</div>
            <div>My assets: ${itemsInCollection.length}</div>
            <div>Floor: ${floor == null ? '-' : `${floor} wFOGO`}</div>
            <div class="bulk">
              <button type="button" class="bulkListBtn" data-collection="${esc(collection)}">Bulk List</button>
              <button type="button" class="bulkDelistBtn" data-collection="${esc(collection)}">Bulk Delist</button>
            </div>
          </div>
          ${groupedCards}
        </section>
      `;
    })
    .join('');

  myAssetsInfoEl.textContent = `Total assets: ${portfolioAssets.length} | Collections: ${byCollection.size}`;
  myAssetsGridEl.innerHTML = collectionBlocks;
}

function updateFooter() {
  const shown = rendered.length;
  pageInfoEl.textContent = `Shown ${shown} / ${totalListings}`;
  loadMoreBtn.hidden = !hasMore;
}

async function getListings(page = 1, mode = 'replace') {
  if (mode === 'replace') {
    statusEl.textContent = 'Syncing...';
  }

  const params = new URLSearchParams();
  params.set('page', String(page));
  params.set('limit', String(pageSize));
  if (filterName.trim()) {
    params.set('q', filterName.trim());
  }
  params.set('sort', sortBy);

  const res = await fetch(`/api/listings?${params.toString()}`, { cache: 'no-store' });
  const payload = await res.json();

  refreshMs = POLL_MS;
  totalListings = Number(payload.total || 0);
  hasMore = !!payload.hasMore;
  currentPage = Number(payload.page || page);

  if (!payload.ok || !payload.data) {
    statusEl.textContent = payload.loading ? 'Initializing...' : 'No data yet';
    updateFooter();
    return;
  }

  const data = payload.data;
  rpcUrl = data.rpc || rpcUrl;
  if (data.collectionFilter) {
    defaultCollection = data.collectionFilter;
  }
  const progress = payload.progress || {};
  statusEl.textContent = payload.loading ? 'Refreshing in background' : 'Live';
  if (!progress.backfillDone) {
    statusEl.textContent = `Building index... ${progress.processedSignatures || 0} tx`;
  }
  countEl.textContent = `${totalListings} active`;
  updatedEl.textContent = `Updated: ${new Date(data.generatedAt).toLocaleTimeString()}`;

  const pageListings = data.listings || [];
  if (mode === 'replace') {
    rendered = [...pageListings];
    renderMarketCards(pageListings, 'replace');
  } else {
    rendered = [...rendered, ...pageListings];
    renderMarketCards(pageListings, 'append');
  }

  updateFooter();
}

async function loadPortfolio() {
  if (!wallets.length) {
    renderPortfolioAssets([]);
    myAssetsInfoEl.textContent = 'Add at least 1 wallet to load assets.';
    return;
  }

  const owners = wallets.map((w) => w.pubkey).join(',');
  statusEl.textContent = 'Loading my assets...';
  const res = await fetch(`/api/portfolio?owners=${encodeURIComponent(owners)}`, { cache: 'no-store' });
  const payload = await res.json();
  if (!payload.ok) {
    throw new Error(payload.error || 'Failed to load portfolio');
  }
  renderPortfolioAssets(payload?.data?.assets || [], payload?.data?.collectionStats || {});
  statusEl.textContent = 'Live';
}

async function manualRefresh() {
  refreshBtn.disabled = true;
  try {
    await fetch('/api/refresh', { method: 'POST' });
    setTimeout(() => {
      getListings(1, 'replace');
      if (currentView === 'assets') {
        loadPortfolio().catch(() => {});
      }
    }, 500);
  } finally {
    refreshBtn.disabled = false;
  }
}

async function onAddWallet() {
  try {
    const secret = parsePrivateKey(privateKeyInputEl.value);
    const keypair = window.solanaWeb3.Keypair.fromSecretKey(secret);
    const pubkey = keypair.publicKey.toBase58();
    if (wallets.some((w) => w.pubkey === pubkey)) {
      alert('Wallet already added');
      return;
    }
    wallets.push({ pubkey, keypair });
    privateKeyInputEl.value = '';
    updateWalletUi();
    if (currentView === 'assets') {
      await loadPortfolio();
    }
  } catch (error) {
    alert(`Invalid private key: ${error.message || error}`);
  }
}

async function onMarketGridClick(event) {
  const buyBtn = event.target.closest('.buyBtn');
  if (!buyBtn) {
    return;
  }
  const listingAddress = buyBtn.getAttribute('data-listing');
  if (!listingAddress) {
    return;
  }
  const activeWallet = getActiveWalletPubkey();
  if (!activeWallet) {
    alert('Add wallet then choose Active wallet for buy.');
    return;
  }

  try {
    await executeTrade('buy', activeWallet, { listingAddress });
    await getListings(1, 'replace');
    if (currentView === 'assets') {
      await loadPortfolio();
    }
  } catch (error) {
    statusEl.textContent = 'buy failed';
    alert(`Buy gagal: ${error.message || error}`);
  }
}

async function onPortfolioGridClick(event) {
  const bulkListBtn = event.target.closest('.bulkListBtn');
  if (bulkListBtn) {
    const collection = bulkListBtn.getAttribute('data-collection') || '';
    if (!collection) {
      return;
    }
    const targets = portfolioAssets.filter((x) =>
      (x.collectionLabel || deriveCollectionLabel(x.name, x.collection)) === collection && x.status !== 'listed'
    );
    if (!targets.length) {
      alert('Tidak ada aset yang bisa di-list di koleksi ini.');
      return;
    }
    const priceText = prompt(`Input price wFOGO untuk bulk listing ${targets.length} aset:`, '10');
    if (priceText === null) {
      return;
    }
    const priceUi = Number(priceText);
    if (!Number.isFinite(priceUi) || priceUi <= 0) {
      alert('Price harus lebih dari 0');
      return;
    }

    try {
      for (const target of targets) {
        await executeTrade('list', target.owner, {
          assetMint: target.mint,
          collection: target.collection || defaultCollection,
          priceUi,
        });
      }
      await loadPortfolio();
      await getListings(1, 'replace');
    } catch (error) {
      statusEl.textContent = 'bulk list failed';
      alert(`Bulk list gagal: ${error.message || error}`);
    }
    return;
  }

  const bulkDelistBtn = event.target.closest('.bulkDelistBtn');
  if (bulkDelistBtn) {
    const collection = bulkDelistBtn.getAttribute('data-collection') || '';
    if (!collection) {
      return;
    }
    const targets = portfolioAssets.filter((x) =>
      (x.collectionLabel || deriveCollectionLabel(x.name, x.collection)) === collection && x.status === 'listed' && x.listingAddress
    );
    if (!targets.length) {
      alert('Tidak ada listing aktif untuk di-delist di koleksi ini.');
      return;
    }

    try {
      for (const target of targets) {
        await executeTrade('delist', target.owner, { listingAddress: target.listingAddress });
      }
      await loadPortfolio();
      await getListings(1, 'replace');
    } catch (error) {
      statusEl.textContent = 'bulk delist failed';
      alert(`Bulk delist gagal: ${error.message || error}`);
    }
    return;
  }

  const delistBtn = event.target.closest('.assetDelistBtn');
  if (delistBtn) {
    const owner = delistBtn.getAttribute('data-owner') || '';
    const listingAddress = delistBtn.getAttribute('data-listing') || '';
    if (!owner || !listingAddress) {
      return;
    }
    try {
      await executeTrade('delist', owner, { listingAddress });
      await loadPortfolio();
      await getListings(1, 'replace');
    } catch (error) {
      statusEl.textContent = 'delist failed';
      alert(`Delist gagal: ${error.message || error}`);
    }
    return;
  }

  const listBtnEl = event.target.closest('.assetListBtn');
  if (!listBtnEl) {
    return;
  }

  const owner = listBtnEl.getAttribute('data-owner') || '';
  const assetMint = listBtnEl.getAttribute('data-mint') || '';
  const collection = listBtnEl.getAttribute('data-collection') || defaultCollection;
  if (!owner || !assetMint) {
    return;
  }

  const priceText = prompt('Input price wFOGO untuk listing:', '10');
  if (priceText === null) {
    return;
  }
  const priceUi = Number(priceText);
  if (!Number.isFinite(priceUi) || priceUi <= 0) {
    alert('Price harus lebih dari 0');
    return;
  }

  try {
    await executeTrade('list', owner, { assetMint, collection, priceUi });
    await loadPortfolio();
    await getListings(1, 'replace');
  } catch (error) {
    statusEl.textContent = 'list failed';
    alert(`List gagal: ${error.message || error}`);
  }
}

function resetAndReload() {
  getListings(1, 'replace').catch(() => {});
}

async function loadMore() {
  if (!hasMore) {
    return;
  }
  loadMoreBtn.disabled = true;
  try {
    await getListings(currentPage + 1, 'append');
  } finally {
    loadMoreBtn.disabled = false;
  }
}

function startPolling() {
  if (timer) clearInterval(timer);
  timer = setInterval(() => {
    getListings(1, 'replace').catch(() => {});
    if (currentView === 'assets' && wallets.length > 0) {
      loadPortfolio().catch(() => {});
    }
  }, POLL_MS);
}

refreshBtn.addEventListener('click', manualRefresh);
loadMoreBtn.addEventListener('click', loadMore);
addWalletBtn.addEventListener('click', onAddWallet);
gridEl.addEventListener('click', onMarketGridClick);
myAssetsGridEl.addEventListener('click', onPortfolioGridClick);
menuMarketBtn.addEventListener('click', () => setView('market'));
menuMyAssetsBtn.addEventListener('click', async () => {
  setView('assets');
  try {
    await loadPortfolio();
  } catch (error) {
    alert(`Gagal load My Assets: ${error.message || error}`);
  }
});
nameFilterEl.addEventListener('input', () => {
  filterName = nameFilterEl.value || '';
  if (filterDebounce) {
    clearTimeout(filterDebounce);
  }
  filterDebounce = setTimeout(resetAndReload, 300);
});
sortSelectEl.addEventListener('change', () => {
  sortBy = sortSelectEl.value || 'newest';
  resetAndReload();
});

(async () => {
  updateWalletUi();
  renderPortfolioAssets([], {});
  await getListings(1, 'replace');
  startPolling();
})();
