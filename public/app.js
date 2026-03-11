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
const menuBoughtBtn = document.getElementById('menuBoughtBtn');
const marketSection = document.getElementById('marketSection');
const myAssetsSection = document.getElementById('myAssetsSection');
const boughtSection = document.getElementById('boughtSection');
const myAssetsGridEl = document.getElementById('myAssetsGrid');
const myAssetsEmptyEl = document.getElementById('myAssetsEmpty');
const myAssetsInfoEl = document.getElementById('myAssetsInfo');
const boughtGridEl = document.getElementById('boughtGrid');
const boughtEmptyEl = document.getElementById('boughtEmpty');
const boughtInfoEl = document.getElementById('boughtInfo');
const bulkListSelectedBtn = document.getElementById('bulkListSelectedBtn');
const bulkDelistSelectedBtn = document.getElementById('bulkDelistSelectedBtn');
const selectAllOwnedBtn = document.getElementById('selectAllOwnedBtn');
const selectAllListedBtn = document.getElementById('selectAllListedBtn');
const clearSelectionBtn = document.getElementById('clearSelectionBtn');

let timer = null;
let refreshMs = 5000;
let currentPage = 1;
let pageSize = 60;
let totalListings = 0;
let hasMore = false;
let rendered = [];
let lastRenderedMarketKey = '';
let boughtActivities = [];
let filterName = '';
let sortBy = 'newest';
let filterDebounce = null;
let rpcUrl = 'https://mainnet.fogo.io';
let defaultCollection = '';
let currentView = 'market';
let portfolioAssets = [];
let portfolioCollectionStats = {};
let portfolioNameStats = {};
let portfolioSummary = { totalEstimatedFloorValue: 0 };
const listPriceDrafts = new Map();
const selectedAssetKeys = new Set();
const POLL_MS = 5000;

function draftKey(owner, mint) {
  return `${owner || ''}:${mint || ''}`;
}

function setPriceDraft(owner, mint, rawPrice) {
  const key = draftKey(owner, mint);
  if (!key || key === ':') {
    return;
  }
  listPriceDrafts.set(key, String(rawPrice || '').trim());
}

function getPriceDraft(owner, mint) {
  const key = draftKey(owner, mint);
  return listPriceDrafts.get(key);
}

function clearPriceDraft(owner, mint) {
  listPriceDrafts.delete(draftKey(owner, mint));
}

function assetSelectionKey(action, owner, mint, listing) {
  return `${action || ''}:${owner || ''}:${mint || ''}:${listing || ''}`;
}

function markSelection(action, owner, mint, listing, checked) {
  const key = assetSelectionKey(action, owner, mint, listing);
  if (!key || key === ':::') {
    return;
  }
  if (checked) {
    selectedAssetKeys.add(key);
  } else {
    selectedAssetKeys.delete(key);
  }
}

function isMarkedSelected(action, owner, mint, listing) {
  return selectedAssetKeys.has(assetSelectionKey(action, owner, mint, listing));
}

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
  return wallets[0]?.pubkey || '';
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
  const isAssets = view === 'assets';
  const isBought = view === 'bought';
  marketSection.hidden = !isMarket;
  myAssetsSection.hidden = !isAssets;
  boughtSection.hidden = !isBought;
  menuMarketBtn.classList.toggle('active', isMarket);
  menuMyAssetsBtn.classList.toggle('active', isAssets);
  menuBoughtBtn.classList.toggle('active', isBought);
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

function renderPortfolioAssets(items, collectionStats = {}, nameStats = {}, summary = {}) {
  portfolioAssets = items || [];
  portfolioCollectionStats = collectionStats || {};
  portfolioNameStats = nameStats || {};
  portfolioSummary = summary || { totalEstimatedFloorValue: 0 };
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
          const nameStat = portfolioNameStats[name] || {};
          const groupFloor = nameStat.floorPriceUi;
          const groupValue = Number(nameStat.estimatedFloorValue || 0);
          const cardsHtml = groupItems
            .map((item) => {
              const actionLabel = item.status === 'listed' ? 'Delist' : 'Listing';
              const actionClass = item.status === 'listed' ? 'assetDelistBtn' : 'assetListBtn';
              const payloadAttr = item.status === 'listed'
                ? `data-listing="${esc(item.listingAddress || '')}"`
                : `data-mint="${esc(item.mint)}" data-collection="${esc(item.collection || defaultCollection || '')}"`;
              const actionType = item.status === 'listed' ? 'delist' : 'list';
              const isSelected = isMarkedSelected(
                actionType,
                item.owner,
                item.mint,
                item.listingAddress || ''
              );
              const floorPrice = groupFloor == null ? '10' : String(groupFloor);
              const persistedPrice = getPriceDraft(item.owner, item.mint);
              const defaultPrice = persistedPrice && persistedPrice.length ? persistedPrice : floorPrice;
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
                    <div class="pick-row">
                      <input type="checkbox" class="assetSelect" data-action="${actionType}" data-owner="${esc(item.owner)}" data-mint="${esc(item.mint)}" data-listing="${esc(item.listingAddress || '')}" data-collection="${esc(item.collection || defaultCollection || '')}" ${isSelected ? 'checked' : ''} />
                      ${item.status === 'listed' ? '<span class="row">Pick to bulk delist</span>' : `<input type="number" class="itemPriceInput" min="0" step="0.000000001" value="${esc(defaultPrice)}" data-owner="${esc(item.owner)}" data-mint="${esc(item.mint)}" /><span class="row">wFOGO</span>`}
                    </div>
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
              <div class="asset-name-title">${esc(name)} (${groupItems.length}) | Floor: ${groupFloor == null ? '-' : `${groupFloor} wFOGO`} | Value: ${groupValue.toFixed(3)} wFOGO</div>
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
          </div>
          ${groupedCards}
        </section>
      `;
    })
    .join('');

  myAssetsInfoEl.textContent = `Total assets: ${portfolioAssets.length} | Collections: ${byCollection.size}`;
  const totalValue = Number(portfolioSummary.totalEstimatedFloorValue || 0);
  myAssetsInfoEl.textContent += ` | Total floor value: ${totalValue.toFixed(3)} wFOGO`;
  myAssetsGridEl.innerHTML = collectionBlocks;
}

function renderBoughtActivities(items) {
  boughtActivities = items || [];
  boughtEmptyEl.hidden = boughtActivities.length > 0;
  if (!boughtActivities.length) {
    boughtGridEl.innerHTML = '';
    boughtInfoEl.textContent = wallets.length ? 'No buy activity yet.' : 'Add wallet first.';
    return;
  }

  boughtGridEl.innerHTML = boughtActivities
    .map((item) => {
      const when = item.createdAt
        ? new Date(Number(item.createdAt) * 1000).toLocaleString()
        : '-';
      return `
        <article class="activity-card">
          <img class="img" src="${esc(item.image || '')}" alt="${esc(item.name || item.assetMint)}" loading="lazy" onerror="this.src='';this.alt='No image'" />
          <div class="body">
            <div class="name">${esc(item.name || item.assetMint)}</div>
            <div class="row">Buyer: ${short(item.buyer)}</div>
            <div class="row">Seller: ${short(item.seller)}</div>
            <div class="row">Mint: ${short(item.assetMint)}</div>
            <div class="row">At: ${esc(when)}</div>
            <div class="price">${item.priceUi == null ? '-' : `${item.priceUi} wFOGO`}</div>
            <div class="actions">
              <a class="rebel-link" target="_blank" rel="noopener noreferrer" href="https://fogoscan.com/tx/${esc(item.signature || '')}">View Tx</a>
            </div>
          </div>
        </article>
      `;
    })
    .join('');

  boughtInfoEl.textContent = `Total bought activity: ${boughtActivities.length}`;
}

function updateFooter() {
  const shown = rendered.length;
  pageInfoEl.textContent = `Shown ${shown} / ${totalListings}`;
  loadMoreBtn.hidden = !hasMore;
}

function toMarketRenderKey(listings) {
  return (listings || [])
    .map((item) => `${item.listingAddress || ''}:${item.priceUi || ''}:${item.assetMint || ''}`)
    .join('|');
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
  const nextRenderKey = toMarketRenderKey(pageListings);
  if (mode === 'replace') {
    const shouldRerender =
      nextRenderKey !== lastRenderedMarketKey ||
      rendered.length !== pageListings.length ||
      currentPage !== 1;
    rendered = [...pageListings];
    if (shouldRerender) {
      renderMarketCards(pageListings, 'replace');
      lastRenderedMarketKey = nextRenderKey;
    }
  } else {
    rendered = [...rendered, ...pageListings];
    renderMarketCards(pageListings, 'append');
    lastRenderedMarketKey = toMarketRenderKey(rendered);
  }

  updateFooter();
}

async function loadPortfolio() {
  if (!wallets.length) {
    renderPortfolioAssets([], {}, {}, {});
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
  renderPortfolioAssets(
    payload?.data?.assets || [],
    payload?.data?.collectionStats || {},
    payload?.data?.nameStats || {},
    payload?.data?.summary || {}
  );
  statusEl.textContent = 'Live';
}

async function loadBoughtActivities() {
  if (!wallets.length) {
    renderBoughtActivities([]);
    return;
  }

  const owners = wallets.map((w) => w.pubkey).join(',');
  statusEl.textContent = 'Loading bought activity...';
  const res = await fetch(`/api/activity/bought?owners=${encodeURIComponent(owners)}&page=1&limit=120`, {
    cache: 'no-store',
  });
  const payload = await res.json();
  if (!payload.ok) {
    throw new Error(payload.error || 'Failed to load bought activity');
  }
  renderBoughtActivities(payload?.data?.activities || []);
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
      } else if (currentView === 'bought') {
        loadBoughtActivities().catch(() => {});
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
    } else if (currentView === 'bought') {
      await loadBoughtActivities();
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
    alert('Add wallet first.');
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

  const card = listBtnEl.closest('.card');
  const priceInput = card?.querySelector('.itemPriceInput');
  const inlinePrice = priceInput?.value || '0';
  const priceUi = Number(inlinePrice);
  if (!Number.isFinite(priceUi) || priceUi <= 0) {
    alert('Price harus lebih dari 0');
    return;
  }

  if (priceInput) {
    setPriceDraft(owner, assetMint, inlinePrice);
  }

  try {
    await executeTrade('list', owner, { assetMint, collection, priceUi });
    clearPriceDraft(owner, assetMint);
    await loadPortfolio();
    await getListings(1, 'replace');
  } catch (error) {
    statusEl.textContent = 'list failed';
    alert(`List gagal: ${error.message || error}`);
  }
}

async function onBulkListSelected() {
  const selected = [...myAssetsGridEl.querySelectorAll('.assetSelect:checked')]
    .filter((el) => el.dataset.action === 'list');
  if (!selected.length) {
    alert('Pilih minimal 1 NFT listable dulu.');
    return;
  }

  try {
    for (const cb of selected) {
      const owner = cb.dataset.owner || '';
      const mint = cb.dataset.mint || '';
      const collection = cb.dataset.collection || defaultCollection;
      const card = cb.closest('.card');
      const priceInput = card?.querySelector('.itemPriceInput');
      const inlinePrice = priceInput?.value || '0';
      const priceUi = Number(inlinePrice);
      if (!owner || !mint || !Number.isFinite(priceUi) || priceUi <= 0) {
        continue;
      }
      if (priceInput) {
        setPriceDraft(owner, mint, inlinePrice);
      }
      await executeTrade('list', owner, { assetMint: mint, collection, priceUi });
      clearPriceDraft(owner, mint);
    }
    await loadPortfolio();
    await getListings(1, 'replace');
  } catch (error) {
    statusEl.textContent = 'bulk list failed';
    alert(`Bulk list gagal: ${error.message || error}`);
  }
}

async function onBulkDelistSelected() {
  const selected = [...myAssetsGridEl.querySelectorAll('.assetSelect:checked')]
    .filter((el) => el.dataset.action === 'delist');
  if (!selected.length) {
    alert('Pilih minimal 1 NFT delistable dulu.');
    return;
  }

  try {
    for (const cb of selected) {
      const owner = cb.dataset.owner || '';
      const listingAddress = cb.dataset.listing || '';
      if (!owner || !listingAddress) {
        continue;
      }
      await executeTrade('delist', owner, { listingAddress });
    }
    await loadPortfolio();
    await getListings(1, 'replace');
  } catch (error) {
    statusEl.textContent = 'bulk delist failed';
    alert(`Bulk delist gagal: ${error.message || error}`);
  }
}

function selectByAction(action, checked) {
  const boxes = [...myAssetsGridEl.querySelectorAll('.assetSelect')];
  for (const box of boxes) {
    if (box.dataset.action === action) {
      box.checked = checked;
      markSelection(
        box.dataset.action || '',
        box.dataset.owner || '',
        box.dataset.mint || '',
        box.dataset.listing || '',
        checked
      );
    }
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
    } else if (currentView === 'bought' && wallets.length > 0) {
      loadBoughtActivities().catch(() => {});
    }
  }, POLL_MS);
}

refreshBtn.addEventListener('click', manualRefresh);
loadMoreBtn.addEventListener('click', loadMore);
addWalletBtn.addEventListener('click', onAddWallet);
gridEl.addEventListener('click', onMarketGridClick);
myAssetsGridEl.addEventListener('click', onPortfolioGridClick);
myAssetsGridEl.addEventListener('input', (event) => {
  const checkEl = event.target.closest('.assetSelect');
  if (checkEl) {
    markSelection(
      checkEl.dataset.action || '',
      checkEl.dataset.owner || '',
      checkEl.dataset.mint || '',
      checkEl.dataset.listing || '',
      !!checkEl.checked
    );
    return;
  }

  const inputEl = event.target.closest('.itemPriceInput');
  if (!inputEl) {
    return;
  }
  const owner = inputEl.getAttribute('data-owner') || '';
  const mint = inputEl.getAttribute('data-mint') || '';
  if (!owner || !mint) {
    return;
  }
  setPriceDraft(owner, mint, inputEl.value || '');
});
bulkListSelectedBtn.addEventListener('click', onBulkListSelected);
bulkDelistSelectedBtn.addEventListener('click', onBulkDelistSelected);
selectAllOwnedBtn.addEventListener('click', () => selectByAction('list', true));
selectAllListedBtn.addEventListener('click', () => selectByAction('delist', true));
clearSelectionBtn.addEventListener('click', () => {
  const boxes = myAssetsGridEl.querySelectorAll('.assetSelect');
  boxes.forEach((x) => {
    x.checked = false;
  });
  selectedAssetKeys.clear();
});
menuMarketBtn.addEventListener('click', () => setView('market'));
menuMyAssetsBtn.addEventListener('click', async () => {
  setView('assets');
  try {
    await loadPortfolio();
  } catch (error) {
    alert(`Gagal load My Assets: ${error.message || error}`);
  }
});
menuBoughtBtn.addEventListener('click', async () => {
  setView('bought');
  try {
    await loadBoughtActivities();
  } catch (error) {
    alert(`Gagal load Bought Activity: ${error.message || error}`);
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
  renderPortfolioAssets([], {}, {}, {});
  await getListings(1, 'replace');
  startPolling();
})();
