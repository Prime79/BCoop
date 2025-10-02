(function(){
  const Icons = {
    search: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>',
    plus: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"></line><line x1="5" y1="12" x2="19" y2="12"></line></svg>',
    eye: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12z"></path><circle cx="12" cy="12" r="3"></circle></svg>',
    edit: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"></path><path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"></path></svg>',
    trash: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="3 6 5 6 21 6"></polyline><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"></path></svg>',
    chevronLeft: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"></polyline></svg>',
    chevronRight: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="9 18 15 12 9 6"></polyline></svg>',
    egg: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3C8 3 5 8 5 12a7 7 0 0 0 14 0c0-4-3-9-7-9z"></path></svg>',
    truck: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13" rx="2"></rect><path d="M16 8h4l3 3v5h-3"></path><circle cx="5.5" cy="17.5" r="2"></circle><circle cx="18.5" cy="17.5" r="2"></circle></svg>',
    analytics: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12h4v8H3zM10 8h4v12h-4zM17 4h4v16h-4z"></path></svg>',
    warehouse: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7l9-4 9 4v12a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path><path d="M3 11h18"></path><path d="M9 11v10"></path><path d="M15 11v10"></path></svg>',
    incubator: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="16" rx="2"></rect><path d="M3 10h18M3 14h18M11 4v16"></path><circle cx="16.5" cy="8" r="0.8"></circle><circle cx="16.5" cy="12" r="0.8"></circle><circle cx="16.5" cy="16" r="0.8"></circle></svg>',
    transfer: () => '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M5 8h9l-3-3"></path><path d="M19 16H10l3 3"></path><path d="M5 12h14"></path></svg>'
  };

  const dateFormatter = new Intl.DateTimeFormat('hu-HU', { year: 'numeric', month: '2-digit', day: '2-digit' });
  const timeFormatter = new Intl.DateTimeFormat('hu-HU', { hour: '2-digit', minute: '2-digit' });
  const numberFormatter = new Intl.NumberFormat('hu-HU');

  function formatDate(value){
    if (!value) return '—';
    const d = typeof value === 'string' ? new Date(value) : value;
    if (Number.isNaN(d.getTime())) return '—';
    return dateFormatter.format(d);
  }

  function computePostHatchState(){
    const trolleys = getHatcherTrolleys();
    const assigned = new Map();
    const planned = new Map();
    trolleys.forEach((item) => {
      if (item.ukSlotNumber) assigned.set(item.ukSlotNumber, item);
      else if (item.plannedUkSlotNumber) planned.set(item.plannedUkSlotNumber, item);
    });

    const slots = POST_HATCH_LAYOUT.map((layout) => {
      const trolley = assigned.get(layout.slotNumber) || null;
      const plannedItem = !trolley ? planned.get(layout.slotNumber) || null : null;
      return {
        slotNumber: layout.slotNumber,
        layoutLabel: layout.label,
        row: layout.row,
        col: layout.col,
        layer: layout.layer,
        trolley,
        planned: plannedItem
      };
    });

    const occupied = slots.filter((slot) => slot.trolley);
    const totalEggs = occupied.reduce((sum, slot) => sum + (slot.trolley.eggQuantity || 0), 0);
    const vaccinatedCount = occupied.filter((slot) => slot.trolley.vaccinated).length;
    const uniqueFlocks = [...new Set(occupied.map((slot) => slot.trolley.flockName).filter(Boolean))];
    const averageFertility = occupied.length ? occupied.reduce((sum, slot) => sum + (slot.trolley.fertilityPercent || 0), 0) / occupied.length : 0;

    return {
      machine: 'Utókeltető blokk',
      engineer: 'Utókeltető csapat',
      slots,
      occupiedCount: occupied.length,
      totalEggs,
      vaccinatedCount,
      flocks: uniqueFlocks,
      averageFertility
    };
  }

  function PostHatchPage(context){
    const state = computePostHatchState();
    const el = document.createElement('div');
    const meta = document.createElement('div');
    const grid = document.createElement('div');
    meta.className = 'incubator-meta';
    grid.className = 'incubator-grid';
    el.append(meta, grid);

    let search = '';
    if (pendingPostHatchSearch) {
      search = pendingPostHatchSearch.toLowerCase();
      pendingPostHatchSearch = null;
    }

    context.setPrimaryAction(() => openSummary());
    context.setSearchHandler((term) => {
      search = term.trim().toLowerCase();
      renderGrid();
    });

    renderMeta();
    renderGrid();

    const searchInput = document.getElementById('global-search');
    if (searchInput && search) searchInput.value = search;

    return { el };

    function renderMeta(){
      meta.innerHTML = `
        <div class="meta-card"><div class="meta-label">Utókeltető blokk</div><div class="meta-value">${state.machine}</div></div>
        <div class="meta-card"><div class="meta-label">Indító mérnök</div><div class="meta-value">${state.engineer}</div></div>
        <div class="meta-card"><div class="meta-label">Aktív kocsik</div><div class="meta-value">${state.occupiedCount} / ${state.slots.length}</div></div>
        <div class="meta-card"><div class="meta-label">Állományok</div><div class="meta-value">${state.flocks.join(', ') || '—'}</div></div>
        <div class="meta-card"><div class="meta-label">Összes tojás</div><div class="meta-value">${numberFormatter.format(state.totalEggs)} db</div></div>
        <div class="meta-card"><div class="meta-label">Oltott kocsik</div><div class="meta-value">${state.vaccinatedCount}</div></div>
      `;
    }

    function renderGrid(){
      grid.innerHTML = '';
      state.slots.forEach((slot) => {
        const card = document.createElement('button');
        card.type = 'button';
        let statusClass = 'empty';
        if (slot.trolley) statusClass = slot.trolley.status;
        else if (slot.planned) statusClass = 'pending';
        card.className = `incubator-slot ${statusClass}`;
        if (!slot.trolley && !slot.planned) card.classList.add('empty');
        card.style.display = '';

        if (search && slot.trolley) {
          const haystack = [
            slot.trolley.trolleyId,
            slot.trolley.flockName,
            slot.trolley.breederFarm,
            slot.trolley.barnId,
            slot.trolley.ukIdentifier,
            slot.layoutLabel
          ].join(' ').toLowerCase();
          if (!haystack.includes(search)) {
            card.style.display = 'none';
          }
        } else if (search && slot.planned) {
          const haystack = [
            slot.planned.trolleyId,
            slot.planned.flockName,
            slot.planned.plannedUkSlotLabel || '',
            slot.layoutLabel
          ].join(' ').toLowerCase();
          if (!haystack.includes(search)) {
            card.style.display = 'none';
          }
        }

        if (slot.trolley) {
          const ekId = slot.trolley.preHatch ? slot.trolley.preHatch.prehatchCartId || '—' : '—';
          const ekSlotLabel = slot.trolley.preHatch ? `#${String(slot.trolley.preHatch.slot).padStart(2, '0')}` : '—';
          card.innerHTML = `
            <div class="slot-header">
              <span>#${String(slot.slotNumber).padStart(2, '0')}</span>
              <span class="slot-badge">UK</span>
            </div>
            <div class="slot-body">
              <div>Áll: <span>${slot.trolley.flockName}</span></div>
              <div>EK dátum: <span>${formatDate(slot.trolley.ekStartDate)}</span></div>
              <div>Leszedés: <span>${formatDate(slot.trolley.plannedHatchDate)}</span></div>
              <div>Ól: <span>${slot.trolley.barnId}</span></div>
              <div>Oltás: <span>${slot.trolley.vaccinated ? 'Oltott' : 'Oltatlan'}</span></div>
              <div>EK kocsi: <span><button type="button" class="link-button" data-ek="${ekId}" data-uk="${slot.trolley.trolleyId}">${ekSlotLabel} / ${ekId}</button></span></div>
            </div>
            <div class="slot-footer">UK azonosító: ${slot.trolley.ukIdentifier}</div>
          `;
          card.querySelector('[data-ek]').addEventListener('click', (event) => {
            event.stopPropagation();
            pendingTransferFilter = {
              ekSlot: ekSlotLabel,
              ukTrolley: slot.trolley.trolleyId,
              openId: slot.trolley.trolleyId
            };
            window.location.hash = '#/hatcher/transfer';
          });
          card.addEventListener('click', () => showDetails(slot));
        } else if (slot.planned) {
          card.innerHTML = `
            <div class="slot-header">
              <span>#${String(slot.slotNumber).padStart(2, '0')}</span>
              <span class="slot-badge">UK</span>
            </div>
            <div class="slot-body">
              <div>Tervezett kocsi: <span>${slot.planned.trolleyId}</span></div>
              <div>Állomány: <span>${slot.planned.flockName}</span></div>
              <div>Várható transzfer: <span>${formatDate(slot.planned.transferDate)}</span></div>
            </div>
            <div class="slot-footer">Állapot: tervezett</div>
          `;
          card.addEventListener('click', () => showPlanned(slot));
        } else {
          card.innerHTML = `
            <div class="slot-header">
              <span>#${String(slot.slotNumber).padStart(2, '0')}</span>
              <span class="slot-badge">Szabad</span>
            </div>
            <div class="slot-body">
              <div>Áll: <span>—</span></div>
              <div>D: <span>—</span></div>
              <div>Ól: <span>—</span></div>
              <div>Oltás: <span>—</span></div>
            </div>
            <div class="slot-footer">Nincs hozzárendelve</div>
          `;
          card.disabled = true;
        }

        grid.appendChild(card);
      });
    }

    function openSummary(){
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Aktív kocsik</div><div class="value">${state.occupiedCount}</div></div>
          <div><div class="label">Összes tojás</div><div class="value">${numberFormatter.format(state.totalEggs)} db</div></div>
          <div><div class="label">Átlag termékenység</div><div class="value">${state.averageFertility.toFixed(1)}%</div></div>
          <div><div class="label">Oltott</div><div class="value">${state.vaccinatedCount}</div></div>
        </div>
      `;
      createDialog({ title: 'Utókeltető összesítés', body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

    function showDetails(slot){
      const trolley = slot.trolley;
      if (!trolley) return;
      const pre = trolley.preHatch;
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">UK kocsi</div><div class="value">${trolley.trolleyId}</div></div>
          <div><div class="label">UK azonosító</div><div class="value">${trolley.ukIdentifier}</div></div>
          <div><div class="label">Állomány</div><div class="value">${trolley.flockName}</div></div>
          <div><div class="label">Szülőpár telep</div><div class="value">${trolley.breederFarm}</div></div>
        </div>
        <div class="slot-summary" style="margin-top:-6px">
          <div><div class="label">Oltás</div><div class="value">${trolley.vaccinated ? 'Oltott' : 'Oltatlan'}</div></div>
          <div><div class="label">Tojás mennyisége</div><div class="value">${numberFormatter.format(trolley.eggQuantity || 0)} db</div></div>
          <div><div class="label">Előkeltető slot</div><div class="value">${pre ? `#${String(pre.slot).padStart(2, '0')}` : '—'}</div></div>
          <div><div class="label">EK kocsi</div><div class="value">${pre ? pre.prehatchCartId || '—' : '—'}</div></div>
        </div>
      `;

      const details = document.createElement('ul');
      details.className = 'slot-timeline';
      details.innerHTML = `
        <li><span>Teljes kocsi</span><span>${trolley.fullBoxes} láda</span></li>
        <li><span>Osztott kocsi</span><span>${trolley.dividedBoxes} láda</span></li>
        <li><span>Indító mérnök</span><span>${trolley.initiator}</span></li>
        <li><span>Istálló</span><span>${trolley.barnId}</span></li>
        <li><span>UK slot</span><span>${slot.layoutLabel}</span></li>
        <li><span>Előkeltető kezdete</span><span>${formatDateTime(trolley.ekStartDate)}</span></li>
        <li><span>Tervezett leszedés</span><span>${formatDateTime(trolley.plannedHatchDate)}</span></li>
        <li><span>Transzfer ideje</span><span>${formatDateTime(trolley.transferDate)}</span></li>
        <li><span>Vakcina</span><span>${trolley.vaccines.map((v) => v.product).join(', ') || '—'}</span></li>
      `;
      body.appendChild(details);

      if (trolley.notes){
        const note = document.createElement('p');
        note.style.marginTop = '12px';
        note.style.fontSize = '13px';
        note.style.color = 'var(--muted)';
        note.textContent = trolley.notes;
        body.appendChild(note);
      }

      createDialog({ title: `Utókeltető slot #${String(slot.slotNumber).padStart(2, '0')}`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

  function showPlanned(slot){
      const info = slot.planned;
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Tervezett kocsi</div><div class="value">${info.trolleyId}</div></div>
          <div><div class="label">Állomány</div><div class="value">${info.flockName}</div></div>
          <div><div class="label">Várható UK slot</div><div class="value">${info.plannedUkSlotLabel || slot.layoutLabel}</div></div>
          <div><div class="label">Várható transzfer</div><div class="value">${formatDateTime(info.transferDate)}</div></div>
        </div>
      `;
      createDialog({ title: `Utókeltető slot #${String(slot.slotNumber).padStart(2, '0')}`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }
  }

  function computePullingData(){
    const trolleys = getHatcherTrolleys().filter((item) => item.ukSlotLabel);
    return trolleys.map((trolley) => ({
      date: trolley.plannedHatchDate || addDaysISO(trolley.transferDate, 21),
      uk: trolley.ukIdentifier || trolley.trolleyId,
      flock: trolley.flockName,
      breeder: trolley.breederFarm,
      startTime: trolley.plannedHatchDate || addDaysISO(trolley.transferDate, 21),
      endTime: addDaysISO(trolley.plannedHatchDate || addDaysISO(trolley.transferDate, 21), 0),
      culled: Math.round((trolley.viableShortfall || 0) * 0.2),
      weight: Math.round((trolley.eggQuantity || 0) * 0.055),
      signature: trolley.initiator
    }));
  }

  function PullingPage(context){
    const el = document.createElement('div');
    const data = computePullingData();
    const meta = document.createElement('div');
    const table = document.createElement('table');
    const filterBar = document.createElement('div');
    const tableWrapper = document.createElement('div');
    meta.className = 'pulling-meta';
    table.className = 'pulling-table';
    tableWrapper.style.overflowX = 'auto';
    filterBar.className = 'filter-bar';
    filterBar.innerHTML = `
      <label>Dátum -tól<input type="date" data-filter="from" /></label>
      <label>Dátum -ig<input type="date" data-filter="to" /></label>
      <button type="button" data-filter="clear">Szűrők törlése</button>
    `;

    table.innerHTML = `
      <thead>
        <tr>
          <th class="sortable" data-key="startTime">Csibe leszedés kezdete</th>
          <th class="sortable" data-key="endTime">Csibe leszedés vége</th>
          <th class="sortable" data-key="uk">UK</th>
          <th class="sortable" data-key="culled">Selejt csibe (db)</th>
          <th class="sortable" data-key="breeder">Szülőpár törzs</th>
          <th class="sortable" data-key="weight">Csibék súlya</th>
          <th class="sortable" data-key="signature">Aláírás</th>
        </tr>
      </thead>
      <tbody></tbody>
    `;

    const tbody = table.querySelector('tbody');
    const fromInput = filterBar.querySelector('[data-filter="from"]');
    const toInput = filterBar.querySelector('[data-filter="to"]');
    const clearBtn = filterBar.querySelector('[data-filter="clear"]');

    const filterState = { from: '', to: '', search: '', sortKey: 'startTime', sortDir: 'asc' };

    const dateRange = computeTransferRange(data.map((row) => ({ transferDate: row.startTime })));
    if (dateRange.from) filterState.from = dateRange.from;
    if (dateRange.to) filterState.to = dateRange.to;
    if (fromInput) fromInput.value = filterState.from;
    if (toInput) toInput.value = filterState.to;

    context.setSearchHandler((term) => {
      filterState.search = term.trim().toLowerCase();
      render();
    });

    fromInput.addEventListener('change', () => {
      filterState.from = fromInput.value || '';
      render();
    });

    toInput.addEventListener('change', () => {
      filterState.to = toInput.value || '';
      render();
    });

    clearBtn.addEventListener('click', () => {
      filterState.from = dateRange.from;
      filterState.to = dateRange.to;
      filterState.search = '';
      const searchInput = document.getElementById('global-search');
      if (searchInput) searchInput.value = '';
      if (fromInput) fromInput.value = filterState.from;
      if (toInput) toInput.value = filterState.to;
      render();
    });

    table.querySelectorAll('th.sortable').forEach((th) => {
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => {
        const key = th.getAttribute('data-key');
        if (filterState.sortKey === key) {
          filterState.sortDir = filterState.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          filterState.sortKey = key;
          filterState.sortDir = 'asc';
        }
        render();
      });
    });

    tableWrapper.appendChild(table);
    el.append(filterBar, meta, tableWrapper);

    render();
    return { el };

    function render(){
      const rows = applyFilters(data);
      renderSummary(rows);
      renderTable(rows);
    }

    function applyFilters(items){
      const fromDate = filterState.from ? new Date(filterState.from) : null;
      const toDate = filterState.to ? new Date(filterState.to) : null;
      if (toDate) toDate.setHours(23, 59, 59, 999);

      return items.filter((row) => {
        const start = row.startTime ? new Date(row.startTime) : null;
        if (fromDate && start && start < fromDate) return false;
        if (toDate && start && start > toDate) return false;
        if (filterState.search) {
          const haystack = [
            formatDateTime(row.startTime),
            formatDateTime(row.endTime),
            row.uk,
            row.breeder,
            row.signature,
            row.flock
          ].join(' ').toLowerCase();
          if (!haystack.includes(filterState.search)) return false;
        }
        return true;
      });
    }

    function renderSummary(rows){
      const totals = rows.reduce((acc, row) => {
        acc.culled += row.culled;
        acc.weight += row.weight;
        return acc;
      }, { culled: 0, weight: 0 });

      meta.innerHTML = `
        <div class="meta-card"><div class="meta-label">Leszedési napok</div><div class="meta-value">${new Set(rows.map((row) => formatDate(row.startTime))).size}</div></div>
        <div class="meta-card"><div class="meta-label">Érintett UK kocsik</div><div class="meta-value">${rows.length}</div></div>
        <div class="meta-card"><div class="meta-label">Selejt csibe összesen</div><div class="meta-value">${numberFormatter.format(totals.culled)} db</div></div>
        <div class="meta-card"><div class="meta-label">Csibék súlya összesen</div><div class="meta-value">${numberFormatter.format(totals.weight)} g</div></div>
      `;
    }

    function renderTable(rows){
      const sorted = [...rows].sort((a, b) => {
        const aVal = getSortValue(a, filterState.sortKey);
        const bVal = getSortValue(b, filterState.sortKey);
        if (aVal < bVal) return filterState.sortDir === 'asc' ? -1 : 1;
        if (aVal > bVal) return filterState.sortDir === 'asc' ? 1 : -1;
        return 0;
      });

      table.querySelectorAll('th.sortable').forEach((th) => {
        th.classList.toggle('active', th.getAttribute('data-key') === filterState.sortKey);
        th.setAttribute('data-direction', filterState.sortKey === th.getAttribute('data-key') ? filterState.sortDir : '');
      });

      tbody.innerHTML = '';
      sorted.forEach((row, index) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${formatDateTime(row.startTime)}</td>
          <td>${formatDateTime(row.endTime)}</td>
          <td><button type="button" class="link-button" data-uk="${row.uk.toLowerCase()}">${row.uk}</button></td>
          <td>${numberFormatter.format(row.culled)} db</td>
          <td>${row.breeder}</td>
          <td>${numberFormatter.format(row.weight)} g</td>
          <td>${row.signature}</td>
        `;
        tbody.appendChild(tr);
        tr.querySelector('[data-uk]').addEventListener('click', () => {
          pendingPostHatchSearch = row.uk;
          window.location.hash = '#/post-hatch';
        });
      });
    }

    function getSortValue(row, key){
      if (key === 'startTime' || key === 'endTime') return row[key] ? new Date(row[key]).getTime() : 0;
      if (key === 'culled' || key === 'weight') return row[key] || 0;
      return (row[key] || '').toString().toLowerCase();
    }
  }
  function formatDateTime(value){
    if (!value) return '—';
    const d = typeof value === 'string' ? new Date(value) : value;
    if (Number.isNaN(d.getTime())) return '—';
    return `${dateFormatter.format(d)} ${timeFormatter.format(d)}`;
  }

  function addDaysISO(value, days){
    if (!value) return null;
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    d.setDate(d.getDate() + days);
    return d.toISOString();
  }

  function buildRange(values, includeTime = false){
    const items = values.filter(Boolean).map((v) => new Date(v)).filter((d) => !Number.isNaN(d.getTime()));
    if (!items.length) return '—';
    const sorted = items.sort((a, b) => a - b);
    const first = includeTime ? formatDateTime(sorted[0]) : formatDate(sorted[0]);
    const last = includeTime ? formatDateTime(sorted[sorted.length - 1]) : formatDate(sorted[sorted.length - 1]);
    return first === last ? first : `${first} – ${last}`;
  }

  let pendingVaccineFilter = null;
  let pendingPostHatchSearch = null;
  let pendingTransferFilter = null;
  let searchHandler = null;
  let primaryHandler = null;

  function renderAppShell(root){
    root.innerHTML = '';
    const app = document.createElement('div');
    app.className = 'app';

    const sidebar = document.createElement('aside');
    sidebar.className = 'sidebar';
    sidebar.innerHTML = `
      <div class="brand">
        <div class="brand-logo"></div>
        <div class="brand-title">MAGIR</div>
      </div>
      <nav class="nav">
        <div class="nav-section">Keltető üzem</div>
        <a href="#/eggs/intake" data-route="#/eggs/intake">${Icons.egg()} Tojás átvétel</a>
        <a href="#/egg-storage" data-route="#/egg-storage">${Icons.warehouse()} Tojásraktár</a>
        <a href="#/eggs/transfer" data-route="#/eggs/transfer">${Icons.truck()} Tojás átrakás</a>
        <a href="#/pre-hatch" data-route="#/pre-hatch">${Icons.incubator()} Előkeltetés</a>
        <a href="#/hatcher/transfer" data-route="#/hatcher/transfer">${Icons.transfer()} Transzfer</a>
        <a href="#/post-hatch" data-route="#/post-hatch">${Icons.transfer()} Utókeltetés</a>
        <a href="#/leszedes" data-route="#/leszedes">${Icons.transfer()} Leszedés</a>
        <a href="#/chick/storage" data-route="#/chick/storage">${Icons.warehouse()} Naposcsibe tárolás</a>
        <a href="#/chick/delivery" data-route="#/chick/delivery">${Icons.truck()} Kiszállítás</a>
        <a href="#/vaccines" data-route="#/vaccines">${Icons.transfer()} Vakcina készlet</a>
        <a href="#/eggs/allocations" data-route="#/eggs/allocations">${Icons.truck()} Allokáció</a>
        <div class="nav-section">Analitika</div>
        <a href="#/analytics" data-route="#/analytics">${Icons.analytics()} Interaktív analítika</a>
      </nav>
    `;

    const topbar = document.createElement('header');
    topbar.className = 'topbar';
    topbar.innerHTML = `
      <div class="topbar-left">
        <div class="page-title">MAGIR</div>
      </div>
      <div class="topbar-right" style="display:flex;align-items:center;gap:10px">
        <label class="search">${Icons.search()} <input id="global-search" placeholder="Keresés..." /></label>
        <button class="btn primary" id="primary-action">${Icons.plus()} Új</button>
      </div>
    `;

    const main = document.createElement('main');
    main.className = 'main';
    main.innerHTML = '<div style="color:var(--muted)">Betöltés...</div>';

    app.appendChild(sidebar);
    app.appendChild(topbar);
    app.appendChild(main);
    root.appendChild(app);

    topbar.querySelector('#global-search').addEventListener('input', (e) => {
      if (searchHandler) searchHandler(e.target.value);
    });

    topbar.querySelector('#primary-action').addEventListener('click', () => {
      if (primaryHandler) primaryHandler();
    });
  }

  function setActiveRoute(hash){
    document.querySelectorAll('.nav a').forEach((link) => {
      if (link.getAttribute('data-route') === hash) link.classList.add('active');
      else link.classList.remove('active');
    });
  }

  function setSearchHandler(fn){
    searchHandler = typeof fn === 'function' ? fn : null;
    const input = document.getElementById('global-search');
    if (input) input.value = '';
  }

  function setPrimaryAction(fn){
    primaryHandler = typeof fn === 'function' ? fn : null;
  }

  function createDialog(options){
    const { title = 'Részletek', body = '', actions = [] } = options || {};
    const backdrop = document.createElement('div');
    backdrop.className = 'dialog-backdrop';

    const dialog = document.createElement('div');
    dialog.className = 'dialog';
    dialog.innerHTML = `
      <div class="dialog-header">
        <div>${title}</div>
        <button class="btn" data-close>Bezár</button>
      </div>
      <div class="dialog-body"></div>
      <div class="dialog-footer"></div>
    `;

    const bodyEl = dialog.querySelector('.dialog-body');
    if (typeof body === 'string') bodyEl.innerHTML = body;
    else bodyEl.appendChild(body);

    const footer = dialog.querySelector('.dialog-footer');
    actions.forEach((action) => {
      const btn = document.createElement('button');
      btn.className = `btn ${action.variant || ''}`.trim();
      btn.textContent = action.label;
      btn.addEventListener('click', () => action.onClick && action.onClick(close));
      footer.appendChild(btn);
    });

    backdrop.appendChild(dialog);
    document.body.appendChild(backdrop);

    function open(){ backdrop.style.display = 'flex'; }
    function close(){ backdrop.remove(); }

    backdrop.addEventListener('click', (event) => {
      if (event.target === backdrop) close();
    });
    dialog.querySelector('[data-close]').addEventListener('click', close);

    return { open, close, el: backdrop };
  }

  function DataTable(config){
    const { columns, getData, onView, onEdit, onDelete } = config;
    const showActions = config.showActions !== false;
    let state = { search: '', sortKey: null, sortDir: 'asc', page: 1, pageSize: 10 };
    let rows = [];

    const root = document.createElement('div');

    const table = document.createElement('table');
    table.className = 'table';
    const thead = document.createElement('thead');
    const tbody = document.createElement('tbody');
    table.appendChild(thead);
    table.appendChild(tbody);

    const trHead = document.createElement('tr');
    columns.forEach((col) => {
      const th = document.createElement('th');
      th.textContent = col.label;
      if (col.sortable) {
        th.className = 'sortable';
        th.title = 'Rendezés';
        th.addEventListener('click', () => {
          state.sortKey = col.key;
          state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
          render();
        });
      }
      trHead.appendChild(th);
    });
    if (showActions) {
      const thActions = document.createElement('th');
      thActions.textContent = 'Műveletek';
      trHead.appendChild(thActions);
    }
    thead.appendChild(trHead);

    const pager = document.createElement('div');
    pager.className = 'pagination';
    pager.innerHTML = `
      <button class="btn" data-prev title="Előző">${Icons.chevronLeft()}</button>
      <span data-page>1</span>/<span data-pages>1</span>
      <button class="btn" data-next title="Következő">${Icons.chevronRight()}</button>
      <span>Elemszám:</span>
      <select class="select" data-size>
        <option>10</option>
        <option>25</option>
        <option>50</option>
      </select>
    `;

    pager.querySelector('[data-prev]').addEventListener('click', () => {
      if (state.page > 1) {
        state.page -= 1;
        render();
      }
    });

    pager.querySelector('[data-next]').addEventListener('click', () => {
      if (state.page < totalPages()) {
        state.page += 1;
        render();
      }
    });

    pager.querySelector('[data-size]').addEventListener('change', (event) => {
      state.pageSize = parseInt(event.target.value, 10);
      state.page = 1;
      render();
    });

    root.appendChild(table);
    root.appendChild(pager);

    function totalPages(){
      return Math.max(1, Math.ceil(filteredRows().length / state.pageSize));
    }

    function filteredRows(){
      const query = state.search.trim().toLowerCase();
      let data = rows;
      if (query) {
        data = data.filter((row) => columns.some((col) => String(row[col.key] ?? '').toLowerCase().includes(query)));
      }
      if (state.sortKey) {
        const key = state.sortKey;
        const dir = state.sortDir;
        data = [...data].sort((a, b) => {
          const av = a[key];
          const bv = b[key];
          if (av == null && bv == null) return 0;
          if (av == null) return -1;
          if (bv == null) return 1;
          const aNorm = typeof av === 'number' ? av : String(av).toLowerCase();
          const bNorm = typeof bv === 'number' ? bv : String(bv).toLowerCase();
          if (aNorm < bNorm) return dir === 'asc' ? -1 : 1;
          if (aNorm > bNorm) return dir === 'asc' ? 1 : -1;
          return 0;
        });
      }
      return data;
    }

    function render(){
      const data = filteredRows();
      const total = totalPages();
      if (state.page > total) state.page = total;
      const start = (state.page - 1) * state.pageSize;
      const pageRows = data.slice(start, start + state.pageSize);

      tbody.innerHTML = '';
      pageRows.forEach((row) => {
      const tr = document.createElement('tr');
      columns.forEach((col) => {
        const td = document.createElement('td');
        let value = row[col.key];
        if (col.render) value = col.render(row);
        if (col.key === 'status') {
          const badge = document.createElement('span');
          const norm = String(value || '').toLowerCase();
          badge.className = 'badge ' + (norm.includes('krit') ? 'danger' : norm.includes('figy') ? 'warn' : 'ok');
          badge.textContent = value;
          td.appendChild(badge);
        } else {
          td.textContent = value;
        }
        tr.appendChild(td);
      });
      if (showActions) {
        const actions = document.createElement('td');
        actions.className = 'table-actions';
        const btnView = document.createElement('button');
        btnView.className = 'btn';
        btnView.innerHTML = Icons.eye();
        btnView.title = 'Megtekintés';
        btnView.addEventListener('click', () => onView && onView(row));

        const btnEdit = document.createElement('button');
        btnEdit.className = 'btn';
        btnEdit.innerHTML = Icons.edit();
        btnEdit.title = 'Szerkesztés';
        btnEdit.addEventListener('click', () => onEdit && onEdit(row));

        const btnDelete = document.createElement('button');
        btnDelete.className = 'btn danger';
        btnDelete.innerHTML = Icons.trash();
        btnDelete.title = 'Törlés';
        btnDelete.addEventListener('click', () => onDelete && onDelete(row));

        actions.append(btnView, btnEdit, btnDelete);
        tr.appendChild(actions);
      }
      tbody.appendChild(tr);
    });

      pager.querySelector('[data-page]').textContent = String(state.page);
      pager.querySelector('[data-pages]').textContent = String(total);
    }

    async function load(){
      rows = await getData();
      render();
    }

    function setSearch(query){
      state.search = query;
      state.page = 1;
      render();
    }

    load();

    return {
      el: root,
      setSearch,
      reload: load,
      getState: () => ({ ...state }),
      setState: (next) => { state = { ...state, ...next }; render(); },
    };
  }

  // Mock API with in-memory data
  const dataset = [];
  const seed = [
    {
      id: 'Balk-01',
      arrival_date: '2024-06-18',
      waybill_no: 'SZ12130',
      plate: 'ABC-120',
      parent_farm: 'Balkány 01',
      incoming_eggs: 111800,
      company_name: 'Balkány Kft.',
      parent_flock_age: '27 hét',
      collection_date: '2024-06-11',
      egg_id: 'SU400',
      storage_date: '2024-06-17',
      current_stock: 111688,
      defect: 112,
      status: 'Normál'
    },
    {
      id: 'HBos-02',
      arrival_date: '2024-06-18',
      waybill_no: 'SZ21730',
      plate: 'NAE-650',
      parent_farm: 'H.BÖSZ 02',
      incoming_eggs: 118900,
      company_name: 'H.BÖSZ Zrt.',
      parent_flock_age: '29 hét',
      collection_date: '2024-06-10',
      egg_id: 'SU420',
      storage_date: '2024-06-16',
      current_stock: 118837,
      defect: 63,
      status: 'Figyelendő'
    },
    {
      id: 'Szek-03',
      arrival_date: '2024-06-19',
      waybill_no: 'SZ21731',
      plate: 'TRE-122',
      parent_farm: 'Székely 03',
      incoming_eggs: 99000,
      company_name: 'Székely Kft.',
      parent_flock_age: '26 hét',
      collection_date: '2024-06-11',
      egg_id: 'SU401',
      storage_date: '2024-06-17',
      current_stock: 99000,
      defect: 0,
      status: 'Normál'
    },
    {
      id: 'Fels-04',
      arrival_date: '2024-06-19',
      waybill_no: 'SZ21732',
      plate: 'GZX-321',
      parent_farm: 'Felsősima 04',
      incoming_eggs: 125400,
      company_name: 'Felsősima Bt.',
      parent_flock_age: '31 hét',
      collection_date: '2024-06-12',
      egg_id: 'SU402',
      storage_date: '2024-06-18',
      current_stock: 125280,
      defect: 120,
      status: 'Kritikus'
    },
    {
      id: 'Bara-05',
      arrival_date: '2024-06-20',
      waybill_no: 'SZ21733',
      plate: 'XYZ-123',
      parent_farm: 'Barabás 05',
      incoming_eggs: 101200,
      company_name: 'Barabás Agro',
      parent_flock_age: '28 hét',
      collection_date: '2024-06-12',
      egg_id: 'SU403',
      storage_date: '2024-06-18',
      current_stock: 101223,
      defect: 23,
      status: 'Normál'
    }
  ];
  dataset.push(...seed);
  for (let i = 6; i <= 35; i += 1) {
    const base = seed[(i - 1) % seed.length];
    dataset.push({
      ...base,
      id: `${base.id.split('-')[0]}-${String(i).padStart(2, '0')}`,
      waybill_no: `SZ${20000 + i}`,
      arrival_date: `2024-06-${String(10 + (i % 20)).padStart(2, '0')}`,
      collection_date: `2024-06-${String(8 + (i % 18)).padStart(2, '0')}`,
      storage_date: `2024-06-${String(9 + (i % 18)).padStart(2, '0')}`,
      parent_flock_age: `${24 + (i % 10)} hét`,
      incoming_eggs: base.incoming_eggs + (i % 7) * 480,
      current_stock: base.incoming_eggs + (i % 7) * 480 - (i % 5) * 36,
      defect: (i % 4) * 12,
      status: i % 11 === 0 ? 'Kritikus' : i % 6 === 0 ? 'Figyelendő' : 'Normál',
    });
  }

  let rows = [...dataset];

  const storageSeed = [
    {
      id: 'storage-1',
      date: '2024/12/18',
      delivery_site: 'Baromfi Coop Kft Barabás',
      waybill_no: '102,379',
      bir_waybill: '29,395,374',
      breed: 'Ross',
      incoming: '126,720',
      outgoing: '126,720',
      stock: '0',
      carts: '36.00',
      release_date: '2024/12/18'
    },
    {
      id: 'storage-2',
      date: '2024/12/20',
      delivery_site: 'Baromfi Coop Kft Barabás',
      waybill_no: '102,380',
      bir_waybill: '29,395,383',
      breed: 'Ross',
      incoming: '105,600',
      outgoing: '—',
      stock: '105,600',
      carts: '30.00',
      release_date: '—'
    },
    {
      id: 'storage-3',
      date: '2024/12/23',
      delivery_site: 'Baromfi Coop Kft Barabás',
      waybill_no: '102,381',
      bir_waybill: '29,395,392',
      breed: 'Ross',
      incoming: '105,600',
      outgoing: '—',
      stock: '211,200',
      carts: '60.00',
      release_date: '—'
    }
  ];

  let storageRows = [...storageSeed];

  const transferSeed = [
    {
      id: 'ATR-2024-001',
      arrival_date: '2024/12/18',
      breeder_site: 'Baromfi Coop Kft · Barabás',
      waybill_no: '102,379',
      flock_age: '27 hét',
      barn_id: 'B-12',
      transfer_date: '2024/12/19',
      prehatch_cart_id: 'ELK-204',
      prehatch_cart_capacity: 7040,
      farm_carts: [
        { id: 'FARM-391', eggs: 3520 },
        { id: 'FARM-392', eggs: 3520 }
      ]
    },
    {
      id: 'ATR-2024-002',
      arrival_date: '2024/12/20',
      breeder_site: 'Baromfi Coop Kft · Nyírkarász',
      waybill_no: '102,386',
      flock_age: '28 hét',
      barn_id: 'N-03',
      transfer_date: '2024/12/21',
      prehatch_cart_id: 'ELK-215',
      prehatch_cart_capacity: 7040,
      farm_carts: [
        { id: 'FARM-401', eggs: 3520 },
        { id: 'FARM-402', eggs: 3520 }
      ]
    },
    {
      id: 'ATR-2024-003',
      arrival_date: '2024/12/23',
      breeder_site: 'Baromfi Coop Kft · Ibrány',
      waybill_no: '102,391',
      flock_age: '29 hét',
      barn_id: 'I-07',
      transfer_date: '2024/12/24',
      prehatch_cart_id: 'ELK-219',
      prehatch_cart_capacity: 7040,
      farm_carts: [
        { id: 'FARM-421', eggs: 3520 },
        { id: 'FARM-422', eggs: 3520 }
      ]
    }
  ];

  const transferRows = transferSeed.flatMap((shipment) =>
    shipment.farm_carts.map((cart, idx) => ({
      id: `${shipment.id}-${idx + 1}`,
      arrival_date: shipment.arrival_date,
      breeder_site: shipment.breeder_site,
      waybill_no: shipment.waybill_no,
      flock_age: shipment.flock_age,
      barn_id: shipment.barn_id,
      transfer_date: shipment.transfer_date,
      farm_cart_id: cart.id,
      farm_cart_capacity: cart.eggs,
      prehatch_cart_id: shipment.prehatch_cart_id,
      prehatch_cart_capacity: shipment.prehatch_cart_capacity
    }))
  );

  function makeSlot(config){
    const farmCarts = (config.farmCarts || []).map((cart) => ({
      id: cart.id,
      eggs: cart.eggs || 0,
      batchId: cart.batchId || '',
      arrivalDate: cart.arrivalDate || null,
      waybill: cart.waybill || '',
      site: cart.site || ''
    }));
    const placementDate = config.placementDate || null;
    const plannedTransferDate = config.plannedTransferDate || addDaysISO(placementDate, 18);
    const expectedHatchDate = config.expectedHatchDate || addDaysISO(placementDate, 21);
    const totalEggs = farmCarts.reduce((sum, cart) => sum + (cart.eggs || 0), 0);
    const batches = config.batches || farmCarts.map((cart) => ({
      id: cart.batchId,
      arrivalDate: cart.arrivalDate,
      eggs: cart.eggs,
      waybill: cart.waybill,
      farmCartId: cart.id,
      site: cart.site
    }));
    return {
      slot: config.slot,
      row: config.row || 1,
      col: config.col || 1,
      layer: config.layer || 1,
      status: config.status || (farmCarts.length ? 'occupied' : 'empty'),
      attention: config.attention || null,
      prehatchCartId: config.prehatchCartId || null,
      placementDate,
      plannedTransferDate,
      expectedHatchDate,
      arrivalSite: config.arrivalSite || '',
      flockName: config.flockName || '',
      barnId: config.barnId || '',
      eggAgeDays: config.eggAgeDays || null,
      eggWeightGr: config.eggWeightGr || null,
      scrapEggs: config.scrapEggs || 0,
      notes: config.notes || '',
      farmCarts,
      batches,
      totalEggs,
      placementShift: config.placementShift || '',
      plannedTransferNote: config.plannedTransferNote || ''
    };
  }

  const preHatchSlots = [
    makeSlot({
      slot: 18,
      row: 1,
      col: 1,
      layer: 2,
      prehatchCartId: 'ELK-401',
      placementDate: '2024-12-02T06:10:00',
      arrivalSite: 'Baromfi Coop Kft · Barabás',
      flockName: 'Barabás 05',
      barnId: 'B-12',
      eggAgeDays: 27,
      eggWeightGr: 63,
      scrapEggs: 18,
      attention: 'warning',
      farmCarts: [
        { id: 'FARM-601', eggs: 3520, batchId: 'BARA-601A', arrivalDate: '2024-11-30', waybill: '102,370', site: 'Barabás 05' },
        { id: 'FARM-602', eggs: 3520, batchId: 'BARA-601B', arrivalDate: '2024-11-30', waybill: '102,371', site: 'Barabás 05' }
      ]
    }),
    makeSlot({
      slot: 15,
      row: 1,
      col: 2,
      layer: 2,
      prehatchCartId: 'ELK-402',
      placementDate: '2024-12-04T05:45:00',
      arrivalSite: 'Baromfi Coop Kft · Nyírkarász',
      flockName: 'Nyírkarász 03',
      barnId: 'N-03',
      eggAgeDays: 26,
      eggWeightGr: 62,
      scrapEggs: 12,
      farmCarts: [
        { id: 'FARM-611', eggs: 3520, batchId: 'NYKA-611A', arrivalDate: '2024-12-02', waybill: '102,380', site: 'Nyírkarász 03' },
        { id: 'FARM-612', eggs: 3520, batchId: 'NYKA-611B', arrivalDate: '2024-12-02', waybill: '102,381', site: 'Nyírkarász 03' }
      ]
    }),
    makeSlot({
      slot: 12,
      row: 1,
      col: 3,
      layer: 2,
      prehatchCartId: 'ELK-403',
      placementDate: '2024-12-06T04:55:00',
      arrivalSite: 'Baromfi Coop Kft · Ibrány',
      flockName: 'Ibrány 07',
      barnId: 'I-07',
      eggAgeDays: 28,
      eggWeightGr: 64,
      scrapEggs: 10,
      farmCarts: [
        { id: 'FARM-621', eggs: 3520, batchId: 'IBR-621A', arrivalDate: '2024-12-04', waybill: '102,388', site: 'Ibrány 07' },
        { id: 'FARM-622', eggs: 3520, batchId: 'IBR-621B', arrivalDate: '2024-12-04', waybill: '102,389', site: 'Ibrány 07' }
      ]
    }),
    makeSlot({
      slot: 9,
      row: 1,
      col: 4,
      layer: 2,
      prehatchCartId: 'ELK-404',
      placementDate: '2024-12-08T07:05:00',
      arrivalSite: 'Baromfi Coop Kft · Tiszabercel',
      flockName: 'Tiszabercel 02',
      barnId: 'T-02',
      eggAgeDays: 25,
      eggWeightGr: 62.5,
      scrapEggs: 6,
      farmCarts: [
        { id: 'FARM-631', eggs: 3520, batchId: 'TSB-631A', arrivalDate: '2024-12-06', waybill: '102,396', site: 'Tiszabercel 02' },
        { id: 'FARM-632', eggs: 3520, batchId: 'TSB-631B', arrivalDate: '2024-12-06', waybill: '102,397', site: 'Tiszabercel 02' }
      ]
    }),
    makeSlot({
      slot: 6,
      row: 1,
      col: 5,
      layer: 2,
      prehatchCartId: 'ELK-405',
      placementDate: '2024-12-10T05:15:00',
      arrivalSite: 'Baromfi Coop Kft · Gáva',
      flockName: 'Gáva 04',
      barnId: 'G-04',
      eggAgeDays: 24,
      eggWeightGr: 61.8,
      scrapEggs: 9,
      attention: 'warning',
      farmCarts: [
        { id: 'FARM-641', eggs: 3520, batchId: 'GAV-641A', arrivalDate: '2024-12-08', waybill: '102,402', site: 'Gáva 04' },
        { id: 'FARM-642', eggs: 3520, batchId: 'GAV-641B', arrivalDate: '2024-12-08', waybill: '102,403', site: 'Gáva 04' }
      ]
    }),
    makeSlot({ slot: 3, row: 1, col: 6, layer: 2, status: 'empty' }),
    makeSlot({
      slot: 17,
      row: 2,
      col: 1,
      layer: 2,
      prehatchCartId: 'ELK-406',
      placementDate: '2024-12-03T06:40:00',
      arrivalSite: 'Baromfi Coop Kft · Barabás',
      flockName: 'Barabás 06',
      barnId: 'B-14',
      eggAgeDays: 27,
      eggWeightGr: 63.2,
      scrapEggs: 14,
      farmCarts: [
        { id: 'FARM-651', eggs: 3520, batchId: 'BARA-651A', arrivalDate: '2024-12-01', waybill: '102,374', site: 'Barabás 06' },
        { id: 'FARM-652', eggs: 3520, batchId: 'BARA-651B', arrivalDate: '2024-12-01', waybill: '102,375', site: 'Barabás 06' }
      ]
    }),
    makeSlot({
      slot: 14,
      row: 2,
      col: 2,
      layer: 2,
      prehatchCartId: 'ELK-407',
      placementDate: '2024-12-05T05:25:00',
      arrivalSite: 'Baromfi Coop Kft · Nyírkarász',
      flockName: 'Nyírkarász 05',
      barnId: 'N-05',
      eggAgeDays: 26,
      eggWeightGr: 62.4,
      scrapEggs: 11,
      farmCarts: [
        { id: 'FARM-661', eggs: 3520, batchId: 'NYKA-661A', arrivalDate: '2024-12-03', waybill: '102,384', site: 'Nyírkarász 05' },
        { id: 'FARM-662', eggs: 3520, batchId: 'NYKA-661B', arrivalDate: '2024-12-03', waybill: '102,385', site: 'Nyírkarász 05' }
      ]
    }),
    makeSlot({
      slot: 11,
      row: 2,
      col: 3,
      layer: 2,
      prehatchCartId: 'ELK-408',
      placementDate: '2024-12-07T06:55:00',
      arrivalSite: 'Baromfi Coop Kft · Ibrány',
      flockName: 'Ibrány 08',
      barnId: 'I-08',
      eggAgeDays: 28,
      eggWeightGr: 64.1,
      scrapEggs: 8,
      farmCarts: [
        { id: 'FARM-671', eggs: 3520, batchId: 'IBR-671A', arrivalDate: '2024-12-05', waybill: '102,390', site: 'Ibrány 08' },
        { id: 'FARM-672', eggs: 3520, batchId: 'IBR-671B', arrivalDate: '2024-12-05', waybill: '102,391', site: 'Ibrány 08' }
      ]
    }),
    makeSlot({
      slot: 8,
      row: 2,
      col: 4,
      layer: 2,
      prehatchCartId: 'ELK-409',
      placementDate: '2024-12-09T04:35:00',
      arrivalSite: 'Baromfi Coop Kft · Tiszavasvári',
      flockName: 'Tiszavasvári 01',
      barnId: 'TV-01',
      eggAgeDays: 25,
      eggWeightGr: 62,
      scrapEggs: 7,
      attention: 'warning',
      farmCarts: [
        { id: 'FARM-681', eggs: 3520, batchId: 'TV-681A', arrivalDate: '2024-12-07', waybill: '102,398', site: 'Tiszavasvári 01' },
        { id: 'FARM-682', eggs: 3520, batchId: 'TV-681B', arrivalDate: '2024-12-07', waybill: '102,399', site: 'Tiszavasvári 01' }
      ]
    }),
    makeSlot({ slot: 5, row: 2, col: 5, layer: 2, status: 'empty' }),
    makeSlot({
      slot: 2,
      row: 2,
      col: 6,
      layer: 2,
      prehatchCartId: 'ELK-410',
      placementDate: '2024-12-12T05:05:00',
      arrivalSite: 'Baromfi Coop Kft · Kántorjánosi',
      flockName: 'Kántorjánosi 02',
      barnId: 'KJ-02',
      eggAgeDays: 24,
      eggWeightGr: 61.5,
      scrapEggs: 5,
      farmCarts: [
        { id: 'FARM-691', eggs: 3520, batchId: 'KJ-691A', arrivalDate: '2024-12-10', waybill: '102,404', site: 'Kántorjánosi 02' },
        { id: 'FARM-692', eggs: 3520, batchId: 'KJ-691B', arrivalDate: '2024-12-10', waybill: '102,405', site: 'Kántorjánosi 02' }
      ]
    }),
    makeSlot({
      slot: 16,
      row: 3,
      col: 1,
      layer: 1,
      prehatchCartId: 'ELK-411',
      placementDate: '2024-12-01T06:50:00',
      arrivalSite: 'Baromfi Coop Kft · Barabás',
      flockName: 'Barabás 04',
      barnId: 'B-10',
      eggAgeDays: 27,
      eggWeightGr: 63.5,
      scrapEggs: 16,
      attention: 'alert',
      farmCarts: [
        { id: 'FARM-701', eggs: 3520, batchId: 'BARA-701A', arrivalDate: '2024-11-29', waybill: '102,362', site: 'Barabás 04' },
        { id: 'FARM-702', eggs: 3520, batchId: 'BARA-701B', arrivalDate: '2024-11-29', waybill: '102,363', site: 'Barabás 04' }
      ]
    }),
    makeSlot({
      slot: 13,
      row: 3,
      col: 2,
      layer: 1,
      prehatchCartId: 'ELK-412',
      placementDate: '2024-12-04T07:20:00',
      arrivalSite: 'Baromfi Coop Kft · Nyírkarász',
      flockName: 'Nyírkarász 04',
      barnId: 'N-04',
      eggAgeDays: 26,
      eggWeightGr: 62.7,
      scrapEggs: 13,
      farmCarts: [
        { id: 'FARM-711', eggs: 3520, batchId: 'NYKA-711A', arrivalDate: '2024-12-02', waybill: '102,382', site: 'Nyírkarász 04' },
        { id: 'FARM-712', eggs: 3520, batchId: 'NYKA-711B', arrivalDate: '2024-12-02', waybill: '102,383', site: 'Nyírkarász 04' }
      ]
    }),
    makeSlot({
      slot: 10,
      row: 3,
      col: 3,
      layer: 1,
      prehatchCartId: 'ELK-413',
      placementDate: '2024-12-07T05:15:00',
      arrivalSite: 'Baromfi Coop Kft · Ibrány',
      flockName: 'Ibrány 06',
      barnId: 'I-06',
      eggAgeDays: 28,
      eggWeightGr: 64.3,
      scrapEggs: 9,
      farmCarts: [
        { id: 'FARM-721', eggs: 3520, batchId: 'IBR-721A', arrivalDate: '2024-12-05', waybill: '102,392', site: 'Ibrány 06' },
        { id: 'FARM-722', eggs: 3520, batchId: 'IBR-721B', arrivalDate: '2024-12-05', waybill: '102,393', site: 'Ibrány 06' }
      ]
    }),
    makeSlot({
      slot: 7,
      row: 3,
      col: 4,
      layer: 1,
      prehatchCartId: 'ELK-414',
      placementDate: '2024-12-09T06:25:00',
      arrivalSite: 'Baromfi Coop Kft · Tiszavasvári',
      flockName: 'Tiszavasvári 03',
      barnId: 'TV-03',
      eggAgeDays: 25,
      eggWeightGr: 62.1,
      scrapEggs: 7,
      farmCarts: [
        { id: 'FARM-731', eggs: 3520, batchId: 'TV-731A', arrivalDate: '2024-12-07', waybill: '102,400', site: 'Tiszavasvári 03' },
        { id: 'FARM-732', eggs: 3520, batchId: 'TV-731B', arrivalDate: '2024-12-07', waybill: '102,401', site: 'Tiszavasvári 03' }
      ]
    }),
    makeSlot({ slot: 4, row: 3, col: 5, layer: 1, status: 'empty' }),
    makeSlot({ slot: 1, row: 3, col: 6, layer: 1, status: 'empty' })
  ];

  function cloneSlotForState(slot){
    return {
      ...slot,
      farmCarts: (slot.farmCarts || []).map((cart) => ({ ...cart })),
      batches: (slot.batches || []).map((batch) => ({ ...batch }))
    };
  }

  let preHatchState = preHatchSlots.map(cloneSlotForState);

  function computeWindow(slots, key){
    const values = slots.map((slot) => slot[key]).filter(Boolean);
    if (!values.length) return { start: null, end: null };
    const sorted = [...values].sort();
    return { start: sorted[0], end: sorted[sorted.length - 1] };
  }

  function computePreHatch(){
    const slots = preHatchState.map(cloneSlotForState);
    const occupied = slots.filter((slot) => slot.status === 'occupied');
    const uniqueFlocks = [...new Set(occupied.map((slot) => slot.flockName).filter(Boolean))];
    const totalEggs = occupied.reduce((sum, slot) => sum + slot.totalEggs, 0);
    const totalScrap = occupied.reduce((sum, slot) => sum + (slot.scrapEggs || 0), 0);
    const eggAges = occupied.map((slot) => slot.eggAgeDays).filter((n) => typeof n === 'number');
    const weights = occupied.map((slot) => slot.eggWeightGr).filter((n) => typeof n === 'number');
    const placementWindow = computeWindow(occupied, 'placementDate');
    const transferWindow = computeWindow(occupied, 'plannedTransferDate');
    const hatchWindow = computeWindow(occupied, 'expectedHatchDate');
    const eggAgeRange = eggAges.length ? `${Math.min(...eggAges)}–${Math.max(...eggAges)} nap` : '—';
    const weightRange = weights.length ? `${Math.min(...weights).toFixed(1)}–${Math.max(...weights).toFixed(1)} g` : '—';

    return {
      machineId: 'EKG-21',
      engineer: 'Kovács Lilla',
      facility: 'Petneháza keltető üzem',
      slots,
      occupiedSlots: occupied,
      uniqueFlocks,
      totalEggs,
      totalScrap,
      eggAgeRange,
      weightRange,
      placementWindow,
      transferWindow,
      hatchWindow
    };
  }

  function getPreHatchSlot(slotNumber){
    const slot = preHatchState.find((item) => item.slot === slotNumber);
    return slot ? cloneSlotForState(slot) : null;
  }

  function setPreHatchSlot(slotNumber, slotData){
    const idx = preHatchState.findIndex((slot) => slot.slot === slotNumber);
    const clone = cloneSlotForState(slotData);
    if (idx >= 0) preHatchState[idx] = clone;
    else preHatchState.push(clone);
  }

  const HATCHER_TRAYS = 80;
  const HATCHER_TRAY_CAPACITY = 88;

  const POST_HATCH_ORDER = [18, 15, 12, 9, 6, 3, 17, 14, 11, 8, 5, 2, 16, 13, 10, 7, 4, 1];
  const POST_HATCH_LAYOUT = POST_HATCH_ORDER.map((slotNumber, idx) => {
    const row = Math.floor(idx / 6) + 1;
    const col = (idx % 6) + 1;
    const label = `Sor ${String.fromCharCode(65 + row - 1)} / ${String(col).padStart(2, '0')}`;
    const layer = row <= 2 ? 2 : 1;
    return { slotNumber, label, row, col, layer };
  });

  const hatcherTrolleysSeed = [
    ...Array.from({ length: 40 }).map((_, index) => {
      const idNum = 40 - index;
      const slot = preHatchSlots[index % preHatchSlots.length];
      const baseDate = new Date('2025-01-04T08:00:00Z');
      baseDate.setDate(baseDate.getDate() - index);
      const transferDate = new Date(baseDate);
      transferDate.setHours(8, 0, 0, 0);
      const candlingDate = new Date(transferDate);
      candlingDate.setHours(transferDate.getHours() - 1);

      const rowGroup = Math.floor(index / 6);
      const colIndex = (index % 6) + 1;
      const ukLabel = `Sor ${String.fromCharCode(65 + rowGroup)} / ${String(colIndex).padStart(2, '0')}`;

      // Fertility target: mostly green (>92%), 3 yellow (88-92), 1 red (<90) within first 20
      let fertilityPercent;
      if (index === 0) fertilityPercent = 95.2;
      else if (index === 5) fertilityPercent = 89.6; // red
      else if ([9, 12, 16].includes(index)) fertilityPercent = 90.8; // yellow
      else fertilityPercent = 93.5 - (index % 4) * 0.4;

      const candled = HATCHER_TRAYS * HATCHER_TRAY_CAPACITY;
      const fertile = Math.round((fertilityPercent / 100) * candled);
      const infertile = Math.round(candled * 0.05);
      const early = Math.round(candled * 0.015);
      const late = Math.round(candled * 0.008);
      const cracked = Math.max(0, candled - (fertile + infertile + early + late));

      const status = fertilityPercent < 90 ? 'bad'
        : fertilityPercent < 92 ? 'warn'
        : 'good';

      const breederFarm = slot ? (slot.arrivalSite || slot.flockName || 'Baromfi Coop Kft') : 'Baromfi Coop Kft';
      const barnId = slot ? (slot.barnId || `B-${String((index % 8) + 1).padStart(2, '0')}`) : `B-${String((index % 8) + 1).padStart(2, '0')}`;
      const flockName = slot ? (slot.flockName || `Állomány ${String.fromCharCode(65 + (index % 26))}`) : `Állomány ${String.fromCharCode(65 + (index % 26))}`;
      const ukIdentifier = `UK-${String(idNum).padStart(3, '0')}`;
      const initiator = ['Takács Péter', 'Szűcs Anna', 'Oláh Mónika'][index % 3];
      const vaccinated = fertilityPercent >= 89;

      return {
        trolleyId: `HTR-${String(idNum).padStart(4, '0')}`,
        hatcherMachine: `HK-${String(7 + (index % 5)).padStart(2, '0')}`,
        position: ukLabel,
        preHatchSlot: slot ? slot.slot : null,
        candlingDate: candlingDate.toISOString(),
        transferDate: transferDate.toISOString(),
        operator: ['Szabó Gábor','Balogh Imre','Kelemen Ákos','Horváth Pál','Varga Edit'][index % 5],
        vaccineOperator: 'Dr. Kiss Nóra',
        vaccineWindow: addDaysISO(transferDate.toISOString(), 0),
      vaccines: [
          {
            product: index % 4 === 0 ? 'Innovax ND-IBD' : 'Cevac Transmune IBD',
            lot: index % 4 === 0 ? 'INX-ND245' : '011M4F1KMA',
            expiry: index % 4 === 0 ? '2025-04-18' : '2025-03-29',
            deliveryNote: '202401120762',
            quantity: fertile,
            type: index % 5 === 0 ? 'day-old' : 'in-ovo',
            withdrawalDays: index % 5 === 0 ? 0 : 0
          }
        ],
        counts: {
          fertile,
          infertile,
          earlyDead: early,
          lateDead: late,
          cracked
        },
        remarks: status === 'bad' ? 'Több veszteség – ellenőrizendő.' : status === 'warn' ? 'Átlag alatti termékenység, figyelni.' : 'Stabil eredmény.',
        status,
        breederFarm,
        barnId,
        flockName,
        ukIdentifier,
        vaccinated,
        initiator,
        eggQuantity: fertile,
        dividedBoxes: 80,
        fullBoxes: 80,
        notes: 'Utókeltető kocsi azonosítási adatok rögzítve.'
      };
    })
  ];

  const vaccineInventory = {
    'in-ovo': [
      { id: 'IO-001', product: 'Cevac Transmune IBD', batch: '011M4F1KMA', expiry: '2025-03-29', deliveryNote: '202401120762', manufacturer: 'Ceva', storedAt: '2-8°C', usedQuantity: 320000, availableQuantity: 180000, totalQuantity: 500000, withdrawalDays: 0, remarks: 'Folyamatban lévő program' },
      { id: 'IO-002', product: 'Cevac MD Rispens', batch: '003L', expiry: '2025-11-30', deliveryNote: '202401220015', manufacturer: 'Ceva', storedAt: '2-8°C', usedQuantity: 210000, availableQuantity: 90000, totalQuantity: 300000, withdrawalDays: 0, remarks: 'Csak in ovo alkalmazásra' },
      { id: 'IO-003', product: 'Innovax ND-IBD', batch: 'INX-ND245', expiry: '2025-04-18', deliveryNote: '202401260022', manufacturer: 'MSD', storedAt: '2-8°C', usedQuantity: 260000, availableQuantity: 140000, totalQuantity: 400000, withdrawalDays: 0, remarks: 'ND + IBD kombináció' }
    ],
    'day-old': [
      { id: 'DO-101', product: 'VITAPEST', batch: '014M3M1KGA', expiry: '2025-06-20', deliveryNote: '202402282436', manufacturer: 'Ceva', storedAt: '2-8°C', usedQuantity: 120000, availableQuantity: 80000, totalQuantity: 200000, withdrawalDays: 21, remarks: 'Élő vírushatású vakcina' },
      { id: 'DO-102', product: 'MASS-L', batch: '010M2S1KMA', expiry: '2025-07-26', deliveryNote: '202402280963', manufacturer: 'Ceva', storedAt: '2-8°C', usedQuantity: 95000, availableQuantity: 105000, totalQuantity: 200000, withdrawalDays: 21, remarks: 'Massachusetts törzs' },
      { id: 'DO-103', product: 'I-BIRD', batch: '036M4U1KRA', expiry: '2025-03-07', deliveryNote: '202402280949', manufacturer: 'Ceva', storedAt: '2-8°C', usedQuantity: 130000, availableQuantity: 70000, totalQuantity: 200000, withdrawalDays: 21, remarks: 'IB elleni vakcina' }
    ]
  };

  function getVaccineInventory(){
    return Object.entries(vaccineInventory).flatMap(([type, list]) =>
      list.map((item) => ({ ...item, type }))
    );
  }

  function getHatcherTrolleys(){
    const mapped = hatcherTrolleysSeed.map((entry) => {
      const totals = entry.counts || {};
      const candled = (totals.fertile || 0) + (totals.infertile || 0) + (totals.earlyDead || 0) + (totals.lateDead || 0);
      const fertileRatio = candled ? (totals.fertile || 0) / candled : 0;
      const fertilityPercent = fertileRatio * 100;
      let status = entry.status || null;
      if (!status) {
        status = 'good';
        if (fertilityPercent < 82) status = 'bad';
        else if (fertilityPercent < 90) status = 'warn';
      }
      const preSlot = entry.preHatchSlot ? getPreHatchSlot(entry.preHatchSlot) : null;
      const totalCapacity = HATCHER_TRAYS * HATCHER_TRAY_CAPACITY;
      const viableShortfall = totalCapacity - (totals.fertile || 0);
      const shortfallPercent = totalCapacity ? (viableShortfall / totalCapacity) * 100 : 0;
      const cracked = totals.cracked || 0;
      const crackedPercent = totalCapacity ? (cracked / totalCapacity) * 100 : 0;
      const transferTimestamp = entry.transferDate ? new Date(entry.transferDate).getTime() : null;
      const ekStart = preSlot ? preSlot.placementDate : entry.transferDate;
      const hatchDate = ekStart ? addDaysISO(ekStart, 21) : addDaysISO(entry.transferDate, 21);
      return {
        ...entry,
        candled,
        totalCapacity,
        fertileRatio,
        fertilityPercent,
        status,
        preHatch: preSlot,
        viableShortfall,
        shortfallPercent,
        crackedPercent,
        cracked,
        vaccines: entry.vaccines || [],
        transferTimestamp,
        ekStartDate: ekStart,
        plannedHatchDate: hatchDate
      };
    });

    const sorted = mapped.sort((a, b) => {
      const at = a.transferTimestamp != null ? a.transferTimestamp : -Infinity;
      const bt = b.transferTimestamp != null ? b.transferTimestamp : -Infinity;
      if (bt === at) return (b.trolleyId > a.trolleyId ? 1 : -1);
      return bt - at;
    });

    sorted.forEach((item, idx) => {
      const layout = POST_HATCH_LAYOUT[idx];
      if (layout) {
        item.ukSlotNumber = layout.slotNumber;
        item.ukSlotLabel = layout.label;
        item.ukSlotRow = layout.row;
        item.ukSlotCol = layout.col;
        item.ukSlotLayer = layout.layer;
        item.plannedUkSlotLabel = null;
        item.plannedUkSlotNumber = null;
        item.displaySlotLabel = layout.label;
      } else {
        const extraIndex = idx - POST_HATCH_LAYOUT.length;
        const baseLayout = POST_HATCH_LAYOUT[idx % POST_HATCH_LAYOUT.length];
        const cycle = Math.floor(extraIndex / POST_HATCH_LAYOUT.length) + 1;
        const letterOffset = baseLayout ? baseLayout.row - 1 + cycle : cycle;
        const label = baseLayout
          ? `Sor ${String.fromCharCode(65 + baseLayout.row - 1 + cycle)} / ${String(baseLayout.col).padStart(2, '0')}`
          : `Sor ${String.fromCharCode(65 + cycle)} / ${String((idx % 6) + 1).padStart(2, '0')}`;
        item.ukSlotNumber = null;
        item.ukSlotLabel = null;
        item.plannedUkSlotLabel = `${label} (terv)`;
        item.plannedUkSlotNumber = baseLayout ? baseLayout.slotNumber : null;
        item.displaySlotLabel = item.plannedUkSlotLabel;
      }
    });

    return sorted;
  }

  // Derived helper: create chick-storage trolleys from post-hatch trolleys
  function getChickTrolleys(){
    const BOXES_PER_TROLLEY = 16 * 2; // 32 láda/kocsi

    const ukTrolleys = getHatcherTrolleys();
    let counter = 1;
    const items = [];

    ukTrolleys.forEach((uk) => {
      // Csak olyan UK kocsikból képezünk csibéskocsit, ahol van kitöltött előkeltető slot
      const pre = uk.preHatch;
      if (!pre || pre.status !== 'occupied') return;
      // Becsült csibeszám (mock környezet): termékeny tojások száma
      let remaining = Math.max(0, Number((uk.counts && uk.counts.fertile) || 0));
      if (remaining === 0) {
        // Legalább egy üres kocsi a láncolhatóság miatt
        const id = `CHK-${String(counter++).padStart(4, '0')}`;
        items.push({
          trolleyId: id,
          boxes: 0,
          chicks: 0,
          full: false,
          breed: 'Ross 308',
          chicksPerBox: 90,
          patternLabel: `32×90`,
          transferDate: uk.plannedHatchDate,
          storageDate: uk.plannedHatchDate,
          plannedDeliveryDate: uk.plannedHatchDate,
          actualDeliveryDate: uk.plannedHatchDate,
          source: uk
        });
        return;
      }

      while (remaining > 0) {
        // Váltsunk 90 és 70 csibe/láda között, hogy legyen 32×70-es kocsi is
        const perBox = (counter % 6 === 0) ? 70 : 90;
        const capacity = BOXES_PER_TROLLEY * perBox;
        let boxesOn;
        let chicksOn;
        let full;
        if (remaining >= capacity) {
          boxesOn = BOXES_PER_TROLLEY;
          chicksOn = capacity;
          full = true;
        } else {
          boxesOn = Math.max(1, Math.ceil(remaining / perBox));
          if (boxesOn > BOXES_PER_TROLLEY) boxesOn = BOXES_PER_TROLLEY;
          chicksOn = Math.min(remaining, boxesOn * perBox);
          full = boxesOn === BOXES_PER_TROLLEY && chicksOn === capacity;
        }

        const id = `CHK-${String(counter++).padStart(4, '0')}`;
        items.push({
          trolleyId: id,
          boxes: boxesOn,
          chicks: chicksOn,
          full,
          breed: 'Ross 308',
          chicksPerBox: perBox,
          patternLabel: `32×${perBox}`,
          // a tervezett kelés napját tekintjük tárolási dátumnak
          transferDate: uk.plannedHatchDate,
          storageDate: uk.plannedHatchDate,
          plannedDeliveryDate: uk.plannedHatchDate,
          actualDeliveryDate: uk.plannedHatchDate,
          source: uk
        });

        remaining -= chicksOn;
        if (chicksOn <= 0) break; // biztosítsuk, hogy ne legyen végtelen ciklus
      }
    });

    // Sort by date desc similar to hatcher page
    return items.sort((a, b) => {
      const at = a.transferDate ? new Date(a.transferDate).getTime() : -Infinity;
      const bt = b.transferDate ? new Date(b.transferDate).getTime() : -Infinity;
      if (bt === at) return (b.trolleyId > a.trolleyId ? 1 : -1);
      return bt - at;
    });
  }

  function delay(ms){
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function getEggIntakeList(){
    await delay(100);
    return [...rows];
  }

  async function createEggIntake(record){
    await delay(80);
    const next = { id: record.id || `NEW-${Date.now()}`, ...record };
    rows = [next, ...rows];
    return next;
  }

  async function updateEggIntake(id, changes){
    await delay(80);
    rows = rows.map((row) => (row.id === id ? { ...row, ...changes } : row));
    return true;
  }

  async function deleteEggIntake(id){
    await delay(80);
    rows = rows.filter((row) => row.id !== id);
    return true;
  }

  async function getEggStorageList(){
    await delay(80);
    return [...storageRows];
  }

  async function getEggTransferList(){
    await delay(80);
    return [...transferRows];
  }

  function PlaceholderPage(title){
    const el = document.createElement('div');
    el.innerHTML = `
      <h2 style="margin:8px 0 16px 0">${title}</h2>
      <div style="color:var(--muted)">Ez egy helykitöltő oldal. A funkció hamarosan érkezik.</div>
    `;
    return { el };
  }

  function AnalyticsPage(context){
    const el = document.createElement('div');
    el.innerHTML = `
      <h2 style="margin:8px 0 16px 0">Interaktív analítika</h2>
      <div style="height: calc(100vh - 160px)">
        <iframe src="../barn_flow_wall_Kaba_interactive.html" style="width:100%;height:100%;border:1px solid var(--line);border-radius:12px;background:var(--bg)"></iframe>
      </div>
    `;
    return { el };
  }

  function EggsIntakePage(context){
    const el = document.createElement('div');

    const columns = [
      { key: 'arrival_date', label: 'Keltetőbe érkezés dátuma', sortable: true },
      { key: 'waybill_no', label: 'Szállítólevél száma', sortable: true },
      { key: 'plate', label: 'Rendszám', sortable: true },
      { key: 'parent_farm', label: 'Szülőpártelep neve', sortable: true },
      { key: 'incoming_eggs', label: 'Beérkező tojás db', sortable: true },
      { key: 'company_name', label: 'Cég név', sortable: true },
      { key: 'parent_flock_age', label: 'Szülőpárálomány kora tojásberakáskor', sortable: true },
      { key: 'collection_date', label: 'Tojásgyűjtés dátuma (legrégebbi)', sortable: true },
      { key: 'egg_id', label: 'Tojás azonosító', sortable: true },
      { key: 'storage_date', label: 'Tojástárolóba kerülés dátuma', sortable: true },
      { key: 'current_stock', label: 'Aktuális tojásmennyiség raktárban', sortable: true },
      { key: 'defect', label: 'Selejt', sortable: true },
      { key: 'status', label: 'Státusz', sortable: true }
    ];

    const table = DataTable({
      columns,
      getData: getEggIntakeList,
      onView: (row) => showDetails(row),
      onEdit: (row) => showEdit(row),
      onDelete: (row) => confirmDelete(row)
    });

    el.appendChild(table.el);

    context.setSearchHandler((term) => table.setSearch(term));
    context.setPrimaryAction(() => showCreate());

    function showDetails(row){
      const body = document.createElement('div');
      body.innerHTML = `
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          ${columns.map((col) => `
            <div class="field">
              <div class="label" style="color:var(--muted)">${col.label}</div>
              <div>${row[col.key] ?? ''}</div>
            </div>
          `).join('')}
        </div>
      `;
      createDialog({ title: `Tétel – ${row.id}`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

    function showCreate(){
      const form = buildForm();
      const dialog = createDialog({
        title: 'Új tétel',
        body: form,
        actions: [
          { label: 'Mégse', onClick: (close) => close() },
          {
            label: 'Mentés',
            variant: 'primary',
            onClick: async (close) => {
              const payload = readForm(form);
              await createEggIntake(payload);
              table.reload();
              close();
            }
          }
        ]
      });
      dialog.open();
    }

    function showEdit(row){
      const form = buildForm(row);
      const dialog = createDialog({
        title: `Szerkesztés – ${row.id}`,
        body: form,
        actions: [
          { label: 'Mégse', onClick: (close) => close() },
          {
            label: 'Mentés',
            variant: 'primary',
            onClick: async (close) => {
              const payload = readForm(form);
              await updateEggIntake(row.id, payload);
              table.reload();
              close();
            }
          }
        ]
      });
      dialog.open();
    }

    function confirmDelete(row){
      const body = document.createElement('div');
      body.textContent = `Biztosan törölni szeretnéd a(z) ${row.id} tételt?`;
      const dialog = createDialog({
        title: 'Törlés megerősítése',
        body,
        actions: [
          { label: 'Mégse', onClick: (close) => close() },
          {
            label: 'Törlés',
            variant: 'danger',
            onClick: async (close) => {
              await deleteEggIntake(row.id);
              table.reload();
              close();
            }
          }
        ]
      });
      dialog.open();
    }

    function buildForm(values){
      const v = values || {};
      const form = document.createElement('form');
      form.innerHTML = `
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
          ${formField('id', 'Azonosító', v.id || '')}
          ${formField('arrival_date', 'Keltetőbe érkezés dátuma', v.arrival_date || '')}
          ${formField('waybill_no', 'Szállítólevél száma', v.waybill_no || '')}
          ${formField('plate', 'Rendszám', v.plate || '')}
          ${formField('parent_farm', 'Szülőpártelep neve', v.parent_farm || '')}
          ${formField('incoming_eggs', 'Beérkező tojás db', v.incoming_eggs || '')}
          ${formField('company_name', 'Cég név', v.company_name || '')}
          ${formField('parent_flock_age', 'Szülőpárálomány kora tojásberakáskor', v.parent_flock_age || '')}
          ${formField('collection_date', 'Tojásgyűjtés dátuma (legrégebbi)', v.collection_date || '')}
          ${formField('egg_id', 'Tojás azonosító', v.egg_id || '')}
          ${formField('storage_date', 'Tojástárolóba kerülés dátuma', v.storage_date || '')}
          ${formField('current_stock', 'Aktuális tojásmennyiség raktárban', v.current_stock || '')}
          ${formField('defect', 'Selejt', v.defect || '')}
          ${formField('status', 'Státusz', v.status || 'Normál')}
        </div>
      `;
      return form;
    }

    function formField(name, label, value){
      const safe = String(value ?? '').replace(/"/g, '&quot;');
      return `
        <label class="field"><span>${label}</span>
          <input name="${name}" value="${safe}" />
        </label>
      `;
    }

    function readForm(form){
      const data = Object.fromEntries(new FormData(form).entries());
      data.incoming_eggs = Number(data.incoming_eggs || 0);
      data.current_stock = Number(data.current_stock || 0);
      data.defect = Number(data.defect || 0);
      return data;
    }

    return { el };
  }

  function EggStoragePage(context){
    const el = document.createElement('div');

    const columns = [
      { key: 'date', label: 'Dátum', sortable: true },
      { key: 'delivery_site', label: 'Beszállítási telep', sortable: true },
      { key: 'waybill_no', label: 'Szállítólevél szám', sortable: true },
      { key: 'bir_waybill', label: 'BÍR szálllev. szám', sortable: true },
      { key: 'breed', label: 'Fajta', sortable: true },
      { key: 'incoming', label: 'Bevétel db', sortable: true },
      { key: 'outgoing', label: 'Kiadás db', sortable: true },
      { key: 'stock', label: 'Tényleges készlet', sortable: true },
      { key: 'release_date', label: 'Indításra kiadott', sortable: true },
      { key: 'carts', label: 'Kocsi / 3520 db', sortable: true }
    ];

    const table = DataTable({
      columns,
      getData: getEggStorageList,
      showActions: false
    });

    el.appendChild(table.el);

    context.setSearchHandler((term) => table.setSearch(term));
    context.setPrimaryAction(() => {
      createDialog({ title: 'Új raktári tétel', body: 'Ez a mock jelenleg csak adatmegtekintésre szolgál.' }).open();
    });

    return { el };
  }

  function EggTransferPage(context){
    const el = document.createElement('div');
    const fmt = new Intl.NumberFormat('hu-HU');

    const columns = [
      { key: 'arrival_date', label: 'Beérkezés dátuma', sortable: true },
      { key: 'breeder_site', label: 'Tenyésztelep', sortable: true },
      { key: 'waybill_no', label: 'Szállítólevél szám', sortable: true },
      { key: 'flock_age', label: 'Állomány kora', sortable: true },
      { key: 'barn_id', label: 'Istálló azonosító', sortable: true },
      { key: 'transfer_date', label: 'Átrakás dátuma', sortable: true },
      { key: 'farm_cart_id', label: 'Farmkocsi azonosító', sortable: true },
      {
        key: 'farm_cart_capacity',
        label: 'Farmkocsi férőhely (db)',
        sortable: true,
        render: (row) => fmt.format(row.farm_cart_capacity)
      },
      { key: 'prehatch_cart_id', label: 'Előkeltető kocsi azonosító', sortable: true },
      {
        key: 'prehatch_cart_capacity',
        label: 'Előkeltető férőhely (db)',
        sortable: true,
        render: (row) => fmt.format(row.prehatch_cart_capacity)
      }
    ];

    const table = DataTable({
      columns,
      getData: getEggTransferList,
      showActions: false
    });

    el.appendChild(table.el);

    context.setSearchHandler((term) => table.setSearch(term));
    context.setPrimaryAction(() => {
      createDialog({
        title: 'Új átrakás',
        body: 'Ez a mock átrakási oldal fix mintaadattal rendelkezik.'
      }).open();
    });

    return { el };
  }

  function PreHatchPage(context){
    const el = document.createElement('div');
    const meta = document.createElement('div');
    const grid = document.createElement('div');
    meta.className = 'incubator-meta';
    grid.className = 'incubator-grid';
    el.append(meta, grid);

    let cards = [];
    let searchValue = '';

    context.setPrimaryAction(() => openSummary());

    render();

    function render(){
      const info = computePreHatch();
      renderMeta(info);
      renderGrid(info);
      context.setSearchHandler((term) => {
        searchValue = term;
        applyFilter();
      });
      const input = document.getElementById('global-search');
      if (input) input.value = searchValue;
      applyFilter();
    }

    function renderMeta(info){
      meta.innerHTML = `
        <div class="meta-card"><div class="meta-label">Előkeltetőgép</div><div class="meta-value">${info.machineId}</div></div>
        <div class="meta-card"><div class="meta-label">Indító mérnök</div><div class="meta-value">${info.engineer}</div></div>
        <div class="meta-card"><div class="meta-label">Berakott állományok</div><div class="meta-value">${info.uniqueFlocks.length ? info.uniqueFlocks.join(', ') : '—'}</div></div>
        <div class="meta-card"><div class="meta-label">Berakott tojások</div><div class="meta-value">${numberFormatter.format(info.totalEggs)} db</div></div>
        <div class="meta-card"><div class="meta-label">Selejt</div><div class="meta-value">${numberFormatter.format(info.totalScrap)} db</div></div>
        <div class="meta-card"><div class="meta-label">Tojások kora</div><div class="meta-value">${info.eggAgeRange}</div></div>
        <div class="meta-card"><div class="meta-label">Tojások súlya</div><div class="meta-value">${info.weightRange}</div></div>
        <div class="meta-card"><div class="meta-label">Berakási ablak</div><div class="meta-value">${buildRange([info.placementWindow.start, info.placementWindow.end], true)}</div></div>
        <div class="meta-card"><div class="meta-label">Tervezett transzferek</div><div class="meta-value">${buildRange([info.transferWindow.start, info.transferWindow.end], true)}</div></div>
        <div class="meta-card"><div class="meta-label">Várható kelés</div><div class="meta-value">${buildRange([info.hatchWindow.start, info.hatchWindow.end], true)}</div></div>
      `;
    }

    function renderGrid(info){
      grid.innerHTML = '';
      cards = [];

      info.slots.forEach((slot) => {
        const card = document.createElement('button');
        card.type = 'button';
        let className = `incubator-slot ${slot.status}`;
        if (slot.attention) className += ` ${slot.attention}`;
        card.className = className;

        if (slot.status === 'occupied') {
          card.innerHTML = `
            <div class="slot-header">
              <span>#${String(slot.slot).padStart(2, '0')}</span>
            </div>
            <div class="slot-body">
              <div>Áll: <span>${slot.flockName}</span></div>
              <div>D: <span>${formatDate(slot.placementDate)}</span></div>
              <div>Ól: <span>${slot.barnId || '—'}</span></div>
              <div>Súly: <span>${slot.eggWeightGr ? `${slot.eggWeightGr} g` : '—'}</span></div>
            </div>
            <div class="slot-footer">Tervezett transzfer: ${formatDate(slot.plannedTransferDate)}</div>
          `;
        } else {
          card.innerHTML = `
            <div class="slot-header">
              <span>#${String(slot.slot).padStart(2, '0')}</span>
              <span class="slot-badge">Szabad</span>
            </div>
            <div class="slot-body">
              <div>Áll: <span>—</span></div>
              <div>D: <span>—</span></div>
              <div>Ól: <span>—</span></div>
              <div>Súly: <span>—</span></div>
            </div>
            <div class="slot-footer">Nincs tervezett transzfer</div>
          `;
        }

        card.addEventListener('click', () => {
          const latest = getPreHatchSlot(slot.slot);
          if (!latest) return;
          if (latest.status === 'occupied') showSlot(latest);
          else openAssignForm(latest);
        });

        grid.appendChild(card);
        cards.push({ slotNumber: slot.slot, card });
      });
    }

    function applyFilter(){
      const q = searchValue.trim().toLowerCase();
      cards.forEach(({ slotNumber, card }) => {
        if (!q) {
          card.style.display = '';
          return;
        }
        const slot = getPreHatchSlot(slotNumber);
        if (!slot) {
          card.style.display = '';
          return;
        }
        const haystack = [
          slot.slot,
          slot.status,
          slot.flockName,
          slot.prehatchCartId,
          slot.barnId,
          slot.arrivalSite,
          slot.eggWeightGr ?? '',
          slot.eggAgeDays ?? '',
          slot.plannedTransferDate,
          ...(slot.farmCarts || []).flatMap((cart) => [cart.id, cart.batchId, cart.site, cart.waybill])
        ].join(' ').toLowerCase();
        card.style.display = haystack.includes(q) ? '' : 'none';
      });
    }

    function openSummary(){
      const info = computePreHatch();
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Berakott pozíciók</div><div class="value">${info.occupiedSlots.length} / ${info.slots.length}</div></div>
          <div><div class="label">Összes tojás</div><div class="value">${numberFormatter.format(info.totalEggs)} db</div></div>
          <div><div class="label">Selejt</div><div class="value">${numberFormatter.format(info.totalScrap)} db</div></div>
          <div><div class="label">Állományok</div><div class="value">${info.uniqueFlocks.length ? info.uniqueFlocks.join(', ') : '—'}</div></div>
        </div>
        <ul class="slot-timeline" style="margin-top:16px">
          <li><span>Berakási ablak</span><span>${buildRange([info.placementWindow.start, info.placementWindow.end], true)}</span></li>
          <li><span>Tervezett transzferek</span><span>${buildRange([info.transferWindow.start, info.transferWindow.end], true)}</span></li>
          <li><span>Várható kelés</span><span>${buildRange([info.hatchWindow.start, info.hatchWindow.end], true)}</span></li>
        </ul>
      `;
      createDialog({ title: 'Előkeltető összesítés', body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

    function showSlot(slot){
      if (slot.status !== 'occupied') {
        createDialog({ title: `Pozíció #${String(slot.slot).padStart(2, '0')}`, body: 'Ez a pozíció jelenleg szabad.' }).open();
        return;
      }

      const body = document.createElement('div');
      const farmList = slot.farmCarts.length ? slot.farmCarts.map((cart) => cart.id).join(', ') : '—';
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Előkeltető kocsi</div><div class="value">${slot.prehatchCartId}</div></div>
          <div><div class="label">Farmkocsik</div><div class="value">${farmList}</div></div>
          <div><div class="label">Állomány</div><div class="value">${slot.flockName}</div></div>
          <div><div class="label">Összes tojás</div><div class="value">${numberFormatter.format(slot.totalEggs)} db</div></div>
        </div>
        <div class="slot-summary" style="margin-top:-6px">
          <div><div class="label">Berakás</div><div class="value">${formatDateTime(slot.placementDate)}</div></div>
          <div><div class="label">Tervezett transzfer</div><div class="value">${formatDateTime(slot.plannedTransferDate)}</div></div>
          <div><div class="label">Várható kelés</div><div class="value">${formatDateTime(slot.expectedHatchDate)}</div></div>
          <div><div class="label">Tojás súlya</div><div class="value">${slot.eggWeightGr ? `${slot.eggWeightGr} g` : '—'}</div></div>
          <div><div class="label">Selejt</div><div class="value">${numberFormatter.format(slot.scrapEggs || 0)} db</div></div>
        </div>
      `;

      if (slot.batches.length){
        const table = document.createElement('div');
        table.className = 'slot-batches';
        table.innerHTML = `
          <table>
            <thead>
              <tr><th>Batch</th><th>Farmkocsi</th><th>Beérkezés</th><th>Szállítólevél</th><th>Mennyiség</th></tr>
            </thead>
            <tbody>
              ${slot.batches.map((batch) => `
                <tr>
                  <td>${batch.id}</td>
                  <td>${batch.farmCartId}</td>
                  <td>${formatDate(batch.arrivalDate)}</td>
                  <td>${batch.waybill || '—'}</td>
                  <td>${numberFormatter.format(batch.eggs)} db</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
        body.appendChild(table);
      }

      const timeline = document.createElement('ul');
      timeline.className = 'slot-timeline';
      timeline.innerHTML = `
        <li><span>Farmkocsik beérkezése</span><span>${buildRange(slot.farmCarts.map((cart) => cart.arrivalDate), false)}</span></li>
        <li><span>Berakás ideje</span><span>${formatDateTime(slot.placementDate)}</span></li>
        <li><span>Tervezett transzfer</span><span>${formatDateTime(slot.plannedTransferDate)}</span></li>
        <li><span>Várható kelés</span><span>${formatDateTime(slot.expectedHatchDate)}</span></li>
      `;
      body.appendChild(timeline);

      // Create and open dialog first
      const dlg = createDialog({ title: `Pozíció #${String(slot.slot).padStart(2, '0')} – ${slot.prehatchCartId}`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] });
      dlg.open();

      // Then append environment chart (safer sizing) and render
      try {
        const chartWrap = document.createElement('div');
        chartWrap.style.cssText = 'margin-top:16px;background:linear-gradient(180deg, rgba(16,27,49,.9), rgba(24,37,63,.85));border:1px solid var(--line);border-radius:12px;padding:12px;';
        const chartHeader = document.createElement('div');
        chartHeader.style.cssText = 'display:flex;justify-content:space-between;align-items:flex-end;margin:6px 8px 8px 8px;';
        const chartTitle = document.createElement('div');
        chartTitle.textContent = 'Előkeltető környezeti profil';
        chartTitle.style.cssText = 'font-weight:600;font-size:18px;';
        const chartHint = document.createElement('div');
        chartHint.textContent = 'Generált demo adatsorok';
        chartHint.style.cssText = 'color:var(--muted);font-size:12px;';
        chartHeader.append(chartTitle, chartHint);
        const canvas = document.createElement('canvas');
        canvas.style.width = '100%';
        canvas.style.height = '360px';
        canvas.style.display = 'block';
        canvas.style.borderRadius = '10px';
        canvas.style.background = 'linear-gradient(180deg, rgba(18,28,48,.6), rgba(18,28,48,.35))';
        chartWrap.style.position = 'relative';
        const tooltip = document.createElement('div');
        tooltip.style.cssText = 'position:absolute;z-index:2;min-width:200px;display:none;pointer-events:none;background:rgba(16,27,49,.98);border:1px solid var(--line);color:var(--text);border-radius:10px;padding:8px 10px;box-shadow:0 8px 28px rgba(0,0,0,.45);font:12px system-ui,Segoe UI,Roboto,Arial,sans-serif;';
        chartWrap.append(chartHeader, canvas, tooltip);
        body.appendChild(chartWrap);

        // Generate demo data and render after next frame for layout
        const data = generateEnvironmentSeries(slot.slot);
        // Attach tooltip element to canvas for interactivity
        canvas.__tooltip = tooltip;
        const draw = () => renderEnvironmentChart(canvas, data);
        const doDraw = () => requestAnimationFrame(draw);
        if (typeof ResizeObserver !== 'undefined') {
          const ro = new ResizeObserver(doDraw);
          ro.observe(chartWrap);
        } else {
          window.addEventListener('resize', draw);
        }
        doDraw();
      } catch (err) {
        console.warn('Chart render failed', err);
      }
    }

    function openAssignForm(slot){
      const form = document.createElement('form');
      form.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Pozíció</div><div class="value">#${String(slot.slot).padStart(2, '0')}</div></div>
          <div><div class="label">Szint</div><div class="value">${slot.layer}</div></div>
        </div>
        <div class="field"><span>Előkeltető kocsi azonosító</span><input name="prehatch_cart_id" required /></div>
        <div class="field"><span>Berakás dátuma és idő</span><input type="datetime-local" name="placement" required /></div>
        <div class="field"><span>Állomány neve</span><input name="flock_name" required /></div>
        <div class="field"><span>Beszállítási telep</span><input name="arrival_site" required /></div>
        <div class="field"><span>Istálló azonosító</span><input name="barn_id" required /></div>
        <div class="field"><span>Berakott tojások kora (nap)</span><input type="number" min="0" name="egg_age" required /></div>
        <div class="field"><span>Tojások súlya (g)</span><input type="number" min="0" step="0.1" name="egg_weight" required /></div>
        <div class="field"><span>Kieső selejt tojás (db)</span><input type="number" min="0" name="scrap_eggs" value="0" /></div>
        <div style="margin-top:14px;font-size:12px;text-transform:uppercase;color:var(--muted)">Farmkocsi 1</div>
        <div class="field"><span>Azonosító</span><input name="farm1_id" required /></div>
        <div class="field"><span>Batch azonosító</span><input name="farm1_batch" required /></div>
        <div class="field"><span>Származási telep</span><input name="farm1_site" /></div>
        <div class="field"><span>Szállítólevél szám</span><input name="farm1_waybill" /></div>
        <div class="field"><span>Beérkezés dátuma</span><input type="date" name="farm1_arrival" required /></div>
        <div class="field"><span>Mennyiség (db)</span><input type="number" min="0" name="farm1_eggs" value="3520" required /></div>
        <div style="margin-top:14px;font-size:12px;text-transform:uppercase;color:var(--muted)">Farmkocsi 2</div>
        <div class="field"><span>Azonosító</span><input name="farm2_id" required /></div>
        <div class="field"><span>Batch azonosító</span><input name="farm2_batch" required /></div>
        <div class="field"><span>Származási telep</span><input name="farm2_site" /></div>
        <div class="field"><span>Szállítólevél szám</span><input name="farm2_waybill" /></div>
        <div class="field"><span>Beérkezés dátuma</span><input type="date" name="farm2_arrival" required /></div>
        <div class="field"><span>Mennyiség (db)</span><input type="number" min="0" name="farm2_eggs" value="3520" required /></div>
      `;

      const arrivalField = form.querySelector('[name="arrival_site"]');
      if (arrivalField) arrivalField.value = slot.arrivalSite || '';
      const flockField = form.querySelector('[name="flock_name"]');
      if (flockField) flockField.value = slot.flockName || '';
      const barnField = form.querySelector('[name="barn_id"]');
      if (barnField) barnField.value = slot.barnId || '';
      const ageField = form.querySelector('[name="egg_age"]');
      if (ageField) ageField.value = slot.eggAgeDays != null ? slot.eggAgeDays : '';
      const weightField = form.querySelector('[name="egg_weight"]');
      if (weightField) weightField.value = slot.eggWeightGr != null ? slot.eggWeightGr : '';
      const scrapField = form.querySelector('[name="scrap_eggs"]');
      if (scrapField && slot.scrapEggs) scrapField.value = slot.scrapEggs;
      const farm1SiteField = form.querySelector('[name="farm1_site"]');
      if (farm1SiteField) farm1SiteField.value = slot.arrivalSite || '';
      const farm2SiteField = form.querySelector('[name="farm2_site"]');
      if (farm2SiteField) farm2SiteField.value = slot.arrivalSite || '';

      const dialog = createDialog({
        title: `Pozíció #${String(slot.slot).padStart(2, '0')} – új berakás`,
        body: form,
        actions: [
          { label: 'Mégse', onClick: (close) => close() },
          {
            label: 'Mentés',
            variant: 'primary',
            onClick: (close) => {
              if (!form.reportValidity()) return;
              const data = Object.fromEntries(new FormData(form).entries());
              const placementISO = toISODateTime(data.placement);
              const farmCarts = [1, 2].map((idx) => ({
                id: data[`farm${idx}_id`].trim(),
                batchId: data[`farm${idx}_batch`].trim(),
                site: (data[`farm${idx}_site`] || data.arrival_site || '').trim(),
                waybill: (data[`farm${idx}_waybill`] || '').trim(),
                arrivalDate: data[`farm${idx}_arrival`] || null,
                eggs: Number(data[`farm${idx}_eggs`] || 0)
              }));

              const updatedSlot = makeSlot({
                slot: slot.slot,
                row: slot.row,
                col: slot.col,
                layer: slot.layer,
                prehatchCartId: data.prehatch_cart_id.trim(),
                placementDate: placementISO,
                arrivalSite: data.arrival_site.trim(),
                flockName: data.flock_name.trim(),
                barnId: data.barn_id.trim(),
                eggAgeDays: Number(data.egg_age || 0),
                eggWeightGr: Number(data.egg_weight || 0),
                scrapEggs: Number(data.scrap_eggs || 0),
                farmCarts
              });

              setPreHatchSlot(updatedSlot.slot, updatedSlot);
              close();
              render();
            }
          }
        ]
      });

      dialog.open();
    }

    function toISODateTime(value){
      if (!value) return null;
      const d = new Date(value);
      return Number.isNaN(d.getTime()) ? null : d.toISOString();
    }

    return { el };
  }

  // ---- Lightweight chart utilities for generated environment series ----
  function generateEnvironmentSeries(seed){
    // 04:00 → 16:00 at 5 minute steps
    const start = new Date();
    start.setHours(4, 0, 0, 0);
    const points = [];
    const rand = mulberry32(Number(seed) * 12345 + 9876);
    const total = (12 * 60) / 5; // 12 hours range displayed on screenshot-like scale
    for (let i = 0; i <= total; i += 1){
      const t = new Date(start.getTime() + i * 5 * 60 * 1000);
      // Humidity oscillates between ~50.6% and ~55.2%
      const rh = 0.525 + 0.026 * Math.sin(i / 3.2 + rand());
      // CO2 oscillates between ~0.12% and ~0.6%
      const co2 = 0.36 + 0.24 * Math.sin(i / 1.6 + rand());
      // Temperature mostly flat with occasional dips
      let temp = 37.62 + 0.08 * (rand() - 0.5);
      if (i < 12) temp -= 0.05 + 0.10 * Math.max(0, (12 - i) / 12); // early stabilization
      if (i > total * 0.55 && i < total * 0.6) temp -= 0.18 * (1 - Math.abs(i - total * 0.575) / (total * 0.025));
      points.push({ t, temp, rh, co2 });
    }
    return points;
  }

  function mulberry32(a){
    let t = a >>> 0;
    return function(){
      t += 0x6D2B79F5;
      let r = Math.imul(t ^ (t >>> 15), 1 | t);
      r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
      return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
    };
  }

  function renderEnvironmentChart(canvas, series){
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(600, Math.floor(rect.width));
    const height = Math.floor(parseFloat(getComputedStyle(canvas).height));
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);
    const ctx = canvas.getContext('2d');
    ctx.scale(dpr, dpr);

    // Colors
    const cBg = '#0b1220';
    const cGrid = 'rgba(255,255,255,0.06)';
    const cText = getComputedStyle(document.documentElement).getPropertyValue('--text') || '#e6f0ff';
    const cPrimary = getComputedStyle(document.documentElement).getPropertyValue('--primary') || '#00ddeb'; // temperature
    const cBlue = 'rgba(90,130,255,1)'; // humidity
    const cBlueFillTop = 'rgba(90,130,255,0.28)';
    const cBlueFillBottom = 'rgba(90,130,255,0.08)';
    const cOrange = 'rgba(250,165,60,1)'; // co2

    // Chart area
    const pad = { left: 64, right: 96, top: 28, bottom: 42 };
    const W = width - pad.left - pad.right;
    const H = height - pad.top - pad.bottom;
    ctx.clearRect(0, 0, width, height);

    // Grid background gradient
    const grd = ctx.createLinearGradient(0, pad.top, 0, pad.top + H);
    grd.addColorStop(0, 'rgba(16,27,49,0.75)');
    grd.addColorStop(1, 'rgba(24,37,63,0.55)');
    ctx.fillStyle = grd;
    ctx.fillRect(pad.left, pad.top, W, H);

    // Scales
    const tMin = series[0].t.getTime();
    const tMax = series[series.length - 1].t.getTime();
    const yTemp = { min: 37.44, max: 37.92 };
    const yRh = { min: 0.504, max: 0.552 };
    const yCo2 = { min: 0.12, max: 0.60 };
    const x = (ts) => pad.left + ((ts - tMin) / (tMax - tMin)) * W;
    const yL = (val) => pad.top + (1 - (val - yTemp.min) / (yTemp.max - yTemp.min)) * H;
    const yR1 = (val) => pad.top + (1 - (val - yRh.min) / (yRh.max - yRh.min)) * H;
    const yR2 = (val) => pad.top + (1 - (val - yCo2.min) / (yCo2.max - yCo2.min)) * H;

    // Grid lines horizontal (temp scale)
    ctx.strokeStyle = cGrid;
    ctx.lineWidth = 1;
    ctx.font = '12px system-ui, sans-serif';
    ctx.fillStyle = cPrimary;
    ctx.textBaseline = 'middle';
    const ticksTemp = [37.44,37.56,37.68,37.80,37.92];
    ticksTemp.forEach((v) => {
      const yy = yL(v);
      ctx.beginPath(); ctx.moveTo(pad.left, yy); ctx.lineTo(pad.left + W, yy); ctx.stroke();
      ctx.fillText(`${v.toFixed(2)}°C`, 8, yy);
    });

    // Right side ticks (RH %)
    ctx.fillStyle = 'rgba(90,130,255,0.95)';
    const ticksRh = [0.504,0.528,0.552];
    ticksRh.forEach((v, i) => {
      const yy = yR1(v);
      ctx.fillText(`${(v*100).toFixed(1)}%`, pad.left + W + 8, yy);
    });

    // Right-most ticks (CO2 %)
    ctx.fillStyle = cOrange;
    const ticksCo2 = [0.12,0.36,0.60];
    ticksCo2.forEach((v) => {
      const yy = yR2(v);
      ctx.fillText(`${v.toFixed(2)}%`, pad.left + W + 56, yy);
    });

    // Axis titles
    ctx.save();
    ctx.fillStyle = cPrimary;
    ctx.font = '13px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.translate(16, pad.top + H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText('Hőmérséklet (°C)', 0, 0);
    ctx.restore();

    ctx.save();
    ctx.fillStyle = cBlue;
    ctx.font = '13px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.translate(pad.left + W + 20, pad.top + H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText('Relatív páratartalom (%)', 0, 0);
    ctx.restore();

    ctx.save();
    ctx.fillStyle = cOrange;
    ctx.font = '13px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.translate(pad.left + W + 76, pad.top + H / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.fillText('CO₂ (%)', 0, 0);
    ctx.restore();

    // X axis labels (hours)
    ctx.fillStyle = cText;
    ctx.textBaseline = 'alphabetic';
    ctx.textAlign = 'center';
    const hourLabels = ['04:00','06:00','08:00','10:00','12:00','14:00','16:00'];
    hourLabels.forEach((h, idx) => {
      const ts = tMin + ((tMax - tMin) / (hourLabels.length - 1)) * idx;
      const xx = x(ts);
      ctx.fillText(h, xx, pad.top + H + 28);
    });

    // Humidity area fill
    const rhPath = new Path2D();
    series.forEach((p, i) => {
      const xx = x(p.t.getTime());
      const yy = yR1(p.rh);
      if (i === 0) rhPath.moveTo(xx, yy);
      else rhPath.lineTo(xx, yy);
    });
    const rhFill = ctx.createLinearGradient(0, pad.top, 0, pad.top + H);
    rhFill.addColorStop(0, cBlueFillTop);
    rhFill.addColorStop(1, cBlueFillBottom);
    ctx.strokeStyle = cBlue;
    ctx.lineWidth = 2;
    ctx.stroke(rhPath);
    // Close area to bottom for fill
    rhPath.lineTo(pad.left + W, pad.top + H);
    rhPath.lineTo(pad.left, pad.top + H);
    rhPath.closePath();
    ctx.fillStyle = rhFill;
    ctx.fill(rhPath);

    // Temperature line
    ctx.strokeStyle = cPrimary;
    ctx.lineWidth = 2;
    ctx.beginPath();
    series.forEach((p, i) => {
      const xx = x(p.t.getTime());
      const yy = yL(p.temp);
      if (i === 0) ctx.moveTo(xx, yy);
      else ctx.lineTo(xx, yy);
    });
    ctx.stroke();

    // CO2 line
    ctx.strokeStyle = cOrange;
    ctx.lineWidth = 2;
    ctx.beginPath();
    series.forEach((p, i) => {
      const xx = x(p.t.getTime());
      const yy = yR2(p.co2);
      if (i === 0) ctx.moveTo(xx, yy);
      else ctx.lineTo(xx, yy);
    });
    ctx.stroke();

    // Legend removed per request (no series labels near x-axis)

    // Store layout for interactivity
    canvas.__pad = pad; canvas.__W = W; canvas.__H = H; canvas.__tMin = tMin; canvas.__tMax = tMax; canvas.__len = series.length;
    canvas.__seriesRef = series;

    // Attach hover listeners once
    if (!canvas.__hoverAttached){
      const onMove = (ev) => {
        const r = canvas.getBoundingClientRect();
        const xx = ev.clientX - r.left;
        const padL = canvas.__pad?.left || 0; const Wv = canvas.__W || 1; const n = (canvas.__len || 1) - 1;
        const ratio = Math.min(1, Math.max(0, (xx - padL) / Wv));
        const idx = Math.round(ratio * n);
        canvas.__hoverIndex = isFinite(idx) ? Math.max(0, Math.min(n, idx)) : null;
        renderEnvironmentChart(canvas, canvas.__seriesRef || series);
      };
      const onLeave = () => {
        canvas.__hoverIndex = null;
        const tt = canvas.__tooltip; if (tt) tt.style.display = 'none';
        renderEnvironmentChart(canvas, canvas.__seriesRef || series);
      };
      canvas.addEventListener('mousemove', onMove);
      canvas.addEventListener('mouseleave', onLeave);
      canvas.__hoverAttached = true;
    }

    // Hover overlay and tooltip
    if (canvas.__hoverIndex != null){
      const idx = Math.max(0, Math.min(series.length - 1, canvas.__hoverIndex));
      const p = series[idx];
      const xx = x(p.t.getTime());
      // Crosshair
      ctx.strokeStyle = 'rgba(255,255,255,0.25)';
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(xx, pad.top); ctx.lineTo(xx, pad.top + H); ctx.stroke();
      // Markers
      const drawDot = (x0, y0, color) => { ctx.fillStyle = color; ctx.beginPath(); ctx.arc(x0, y0, 3, 0, Math.PI * 2); ctx.fill(); };
      drawDot(xx, yL(p.temp), cPrimary);
      drawDot(xx, yR1(p.rh), cBlue);
      drawDot(xx, yR2(p.co2), cOrange);

      // Tooltip DOM
      const tt = canvas.__tooltip;
      if (tt){
        const time = p.t; const hh = String(time.getHours()).padStart(2, '0'); const mm = String(time.getMinutes()).padStart(2, '0');
        const fmt = (n, f=2) => n.toFixed(f);
        tt.innerHTML = `
          <div style="font-weight:600;margin-bottom:6px">${hh}:${mm}</div>
          <div style="display:flex;gap:10px;align-items:center;margin:2px 0"><span style="width:10px;height:10px;border-radius:2px;background:${cPrimary};display:inline-block"></span><span>Hőmérséklet:</span><strong>${fmt(p.temp,2)}°C</strong></div>
          <div style="display:flex;gap:10px;align-items:center;margin:2px 0"><span style="width:10px;height:10px;border-radius:2px;background:${cBlue};display:inline-block"></span><span>Relatív páratartalom:</span><strong>${fmt(p.rh*100,1)}%</strong></div>
          <div style="display:flex;gap:10px;align-items:center;margin:2px 0"><span style="width:10px;height:10px;border-radius:2px;background:${cOrange};display:inline-block"></span><span>CO₂:</span><strong>${fmt(p.co2,2)}%</strong></div>
        `;
        // Position tooltip near the vertical line
        const wrap = canvas.parentElement; const wrapRect = wrap.getBoundingClientRect();
        const cRect = canvas.getBoundingClientRect();
        const leftInWrap = (cRect.left - wrapRect.left) + xx + 12; // offset to the right of line
        const topInWrap = (cRect.top - wrapRect.top) + pad.top + 8;
        tt.style.left = `${Math.max(8, Math.min(wrapRect.width - 220, leftInWrap))}px`;
        tt.style.top = `${topInWrap}px`;
        tt.style.display = 'block';
      }
    }
  }

  function drawLegendItem(ctx, x, y, color, label){
    ctx.lineWidth = 4;
    ctx.strokeStyle = color;
    ctx.beginPath(); ctx.moveTo(x, y); ctx.lineTo(x + 24, y); ctx.stroke();
    ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text') || '#e6f0ff';
    ctx.font = '12px system-ui, sans-serif';
    ctx.textBaseline = 'middle';
    ctx.fillText(label, x + 32, y);
  }

  function HatcherTransferPage(context){
    const el = document.createElement('div');
    const filterBar = document.createElement('div');
    const meta = document.createElement('div');
    const grid = document.createElement('div');
    filterBar.className = 'filter-bar';
    filterBar.innerHTML = `
      <label>Dátum -tól<input type="date" data-filter="from" /></label>
      <label>Dátum -ig<input type="date" data-filter="to" /></label>
      <button type="button" data-filter="clear">Szűrő törlése</button>
    `;
    meta.className = 'trolley-meta';
    grid.className = 'trolley-grid';
    el.append(filterBar, meta, grid);

    const fromInput = filterBar.querySelector('[data-filter="from"]');
    const toInput = filterBar.querySelector('[data-filter="to"]');
    const clearBtn = filterBar.querySelector('[data-filter="clear"]');

    let searchValue = '';
    let searchDisplayValue = '';
    if (pendingTransferFilter && pendingTransferFilter.ukTrolley) {
      searchValue = pendingTransferFilter.ukTrolley.toLowerCase();
      searchDisplayValue = pendingTransferFilter.ukTrolley;
    }
    let filterState = { from: '', to: '' };
    let cards = [];
    let visibleData = [];

    fromInput.addEventListener('change', () => {
      filterState.from = fromInput.value || '';
      render();
    });
    toInput.addEventListener('change', () => {
      filterState.to = toInput.value || '';
      render();
    });
    clearBtn.addEventListener('click', () => {
      filterState = { from: '', to: '' };
      searchValue = '';
      searchDisplayValue = '';
      pendingTransferFilter = null;
      const searchInput = document.getElementById('global-search');
      if (searchInput) searchInput.value = '';
      render();
    });

    context.setSearchHandler((term) => {
      searchValue = term;
      searchDisplayValue = term;
      render();
    });
    context.setPrimaryAction(() => openSummary());
    render();

    function render(){
      const data = getHatcherTrolleys();
      const range = computeTransferRange(data);
      if (!filterState.from && range.from) filterState.from = range.from;
      if (!filterState.to && range.to) filterState.to = range.to;
      const filtered = applyDataFilters(data);
      visibleData = filtered;
      renderMeta(filtered);
      renderGrid(filtered);
      openPendingCard();
      if (fromInput) fromInput.value = filterState.from;
      if (toInput) toInput.value = filterState.to;
      const searchInput = document.getElementById('global-search');
      if (searchInput && document.activeElement !== searchInput) {
        const display = searchDisplayValue || searchValue;
        if (display !== undefined) searchInput.value = display;
      }
    }

    function renderMeta(data){
      const totals = data.reduce((acc, item) => {
        acc.fertile += item.counts.fertile || 0;
        acc.infertile += item.counts.infertile || 0;
        acc.early += item.counts.earlyDead || 0;
        acc.late += item.counts.lateDead || 0;
        acc.cracked += item.counts.cracked || 0;
        acc.capacity += item.totalCapacity || 0;
        return acc;
      }, { fertile: 0, infertile: 0, early: 0, late: 0, cracked: 0, capacity: 0 });
      const viablePercent = totals.capacity ? (totals.fertile / totals.capacity) * 100 : 0;
      const lossPercent = totals.capacity ? ((totals.capacity - totals.fertile) / totals.capacity) * 100 : 0;
      const avgCandling = data.length ? (data.reduce((sum, item) => sum + item.fertilityPercent, 0) / data.length) : 0;
      meta.innerHTML = `
        <div class="meta-card"><div class="meta-label">Utókeltető kocsik</div><div class="meta-value">${data.length}</div></div>
        <div class="meta-card"><div class="meta-label">Termékeny tojások</div><div class="meta-value">${numberFormatter.format(totals.fertile)} db</div></div>
        <div class="meta-card"><div class="meta-label">Átlagos termékenység</div><div class="meta-value">${avgCandling.toFixed(1)}%</div></div>
        <div class="meta-card"><div class="meta-label">Nem termékeny összesen</div><div class="meta-value">${numberFormatter.format(totals.infertile + totals.early + totals.late)} db</div></div>
        <div class="meta-card"><div class="meta-label">Selejt / Repedt</div><div class="meta-value">${numberFormatter.format(totals.cracked)} db</div></div>
        <div class="meta-card"><div class="meta-label">Kapacitás kihasználtság</div><div class="meta-value">${viablePercent.toFixed(1)}%</div></div>
        <div class="meta-card"><div class="meta-label">Veszteség</div><div class="meta-value">${lossPercent.toFixed(1)}%</div></div>
      `;
    }

    function renderGrid(data){
      grid.innerHTML = '';
      cards = [];
      data.forEach((item) => {
        const card = document.createElement('button');
        card.type = 'button';
        card.className = `trolley-card ${item.status}`;
        const badgeStatus = item.status === 'good' ? 'zöld' : item.status === 'warn' ? 'sárga' : item.status === 'bad' ? 'piros' : item.status === 'pending' ? 'folyamatban' : 'szabad';
        const deltaClass = item.status === 'good' ? 'delta-good' : item.status === 'warn' ? 'delta-warn' : item.status === 'bad' ? 'delta-bad' : '';

        if (item.status === 'pending' || item.status === 'available') {
          card.innerHTML = `
            <div class="trolley-header">
              <h3>${item.trolleyId}</h3>
              <div class="trolley-badges">
                <span class="pill">${item.hatcherMachine}</span>
                <span class="pill">${item.displaySlotLabel || 'Nincs UK slot'}</span>
              </div>
            </div>
            <div class="trolley-body" style="grid-template-columns:1fr">
              <div><span class="label">Állapot</span><span class="value">${item.status === 'pending' ? 'Előkészítés alatt' : 'Szabad hely'}</span></div>
            </div>
            <div class="trolley-footer">
              <span>${item.remarks || ''}</span>
              <span>${item.preHatchSlot ? `Előkelt: #${item.preHatchSlot}` : ''}</span>
            </div>
          `;
          card.dataset.trolleyId = item.trolleyId;
          if (item.status === 'available') {
            card.disabled = true;
            card.style.cursor = 'default';
          } else {
            card.addEventListener('click', () => showDetails(item));
          }
        } else {
          card.innerHTML = `
            <div class="trolley-header">
              <h3>${item.trolleyId}</h3>
              <div class="trolley-badges">
                <span class="pill">${item.hatcherMachine}</span>
                <span class="pill">${item.displaySlotLabel || 'Nincs UK slot'}</span>
              </div>
            </div>
            <div class="trolley-body">
              <div><span class="label">Termékeny tojások</span><span class="value">${numberFormatter.format(item.counts.fertile)} db</span></div>
              <div><span class="label">Termékenység</span><span class="value">${item.fertilityPercent.toFixed(1)}%</span></div>
              <div><span class="label">Transzfer / Candling</span><span class="value">${formatDate(item.transferDate)} / ${formatDate(item.candlingDate)}</span></div>
              <div><span class="label">EK-slot</span><span class="value">${item.preHatch ? `#${String(item.preHatch.slot).padStart(2,'0')}` : '—'} / ${item.preHatch ? (item.preHatch.prehatchCartId || '—') : '—'}</span></div>
              <div><span class="label">UK azonosító</span><span class="value">${item.ukIdentifier || '—'}</span></div>
              <div><span class="label">UK slot</span><span class="value">${item.ukSlotLabel || item.plannedUkSlotLabel || 'Tervezés alatt'}</span></div>
              <div><span class="label">Veszteség</span><span class="value ${deltaClass}">${numberFormatter.format(item.viableShortfall)} db (${item.shortfallPercent.toFixed(1)}%)</span></div>
            </div>
            <div class="trolley-footer">
              <span>Status: ${badgeStatus}</span>
              <span>${item.vaccines.map((v) => v.product).join(', ') || 'Nincs vakcina'}</span>
            </div>
          `;
          card.dataset.trolleyId = item.trolleyId;
          card.addEventListener('click', () => showDetails(item));
        }

        grid.appendChild(card);
        cards.push({ card, data: item });
      });
    }

    function applyDataFilters(items){
      const fromDate = filterState.from ? new Date(filterState.from) : null;
      const toDate = filterState.to ? new Date(filterState.to) : null;
      if (toDate) toDate.setHours(23, 59, 59, 999);
      const q = searchValue.trim().toLowerCase();

      const matchesBase = (item) => {
        const transferDate = item.transferDate ? new Date(item.transferDate) : null;
        if (fromDate && (!transferDate || transferDate < fromDate)) return false;
        if (toDate && (!transferDate || transferDate > toDate)) return false;
        if (!q) return true;
        const haystack = [
          item.trolleyId,
          item.hatcherMachine,
          item.displaySlotLabel,
          item.operator,
          item.vaccineOperator,
          formatDate(item.transferDate),
          formatDate(item.candlingDate),
          ...(item.vaccines || []).map((v) => `${v.product} ${v.lot}`),
          item.preHatch && item.preHatch.flockName,
          item.preHatch && item.preHatch.prehatchCartId
        ].filter(Boolean).join(' ').toLowerCase();
        return haystack.includes(q);
      };

      let filtered = items.filter(matchesBase);
      if (pendingTransferFilter && pendingTransferFilter.ukTrolley) {
        filtered = filtered.filter((item) => item.trolleyId === pendingTransferFilter.ukTrolley);
      }
      if (pendingTransferFilter && pendingTransferFilter.ekSlot) {
        filtered = filtered.filter((item) => item.preHatch && `#${String(item.preHatch.slot).padStart(2, '0')}` === pendingTransferFilter.ekSlot);
      }

      if (filtered.length === 0 && (pendingTransferFilter || q)) {
        pendingTransferFilter = null;
        filtered = items.filter(matchesBase);
      }

      return filtered;
    }

    function openPendingCard(){
      if (!pendingTransferFilter || !pendingTransferFilter.openId) return;
      const match = cards.find(({ data }) => data.trolleyId === pendingTransferFilter.openId);
      if (match) {
        const { data } = match;
        pendingTransferFilter.openId = null;
        showDetails(data);
      }
    }

    function openSummary(){
      const data = visibleData;
      const totals = data.reduce((acc, item) => {
        acc.fertile += item.counts.fertile || 0;
        acc.infertile += item.counts.infertile || 0;
        acc.early += item.counts.earlyDead || 0;
        acc.late += item.counts.lateDead || 0;
        acc.cracked += item.counts.cracked || 0;
        return acc;
      }, { fertile: 0, infertile: 0, early: 0, late: 0, cracked: 0 });
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Kocsik</div><div class="value">${data.length}</div></div>
          <div><div class="label">Termékeny</div><div class="value">${numberFormatter.format(totals.fertile)} db</div></div>
          <div><div class="label">Nem termékeny</div><div class="value">${numberFormatter.format(totals.infertile)} db</div></div>
          <div><div class="label">Korai elhalt</div><div class="value">${numberFormatter.format(totals.early)} db</div></div>
          <div><div class="label">Kései elhalt</div><div class="value">${numberFormatter.format(totals.late)} db</div></div>
          <div><div class="label">Repedt</div><div class="value">${numberFormatter.format(totals.cracked)} db</div></div>
        </div>
      `;
      createDialog({ title: 'Transzfer összesítés', body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

    function showDetails(item){
      if (item.status === 'available') {
        createDialog({ title: `Transzfer – ${item.trolleyId}`, body: 'Szabad, beosztatlan kocsi pozíció.' }).open();
        return;
      }
      if (item.status === 'pending') {
        const bodyPending = document.createElement('div');
        bodyPending.innerHTML = `
          <div class="slot-summary">
            <div><div class="label">Utókeltető kocsi</div><div class="value">${item.trolleyId}</div></div>
            <div><div class="label">Pozíció</div><div class="value">${item.hatcherMachine} · ${item.displaySlotLabel || '—'}</div></div>
          </div>
          <p style="margin-top:12px;color:var(--muted);font-size:13px">${item.remarks || 'Előkészítés alatt.'}</p>
        `;
        createDialog({ title: `Transzfer – ${item.trolleyId}`, body: bodyPending, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
        return;
      }

      const pre = item.preHatch;
      const totalCandled = item.candled || 0;
      const percent = (value) => totalCandled ? `${((value / totalCandled) * 100).toFixed(1)}%` : '—';
      const body = document.createElement('div');
      const breederSite = pre ? (pre.arrivalSite || (pre.farmCarts[0] && pre.farmCarts[0].site) || '—') : '—';
      const barn = pre ? (pre.barnId || '—') : '—';
      const flockAge = pre && pre.eggAgeDays != null ? `${pre.eggAgeDays} nap` : '—';
      const farmArrivalRange = pre ? buildRange(pre.farmCarts.map((cart) => cart.arrivalDate), false) : '—';
      const prePlacement = pre ? formatDateTime(pre.placementDate) : '—';
      const prePosition = pre ? `#${String(pre.slot).padStart(2, '0')} (${pre.prehatchCartId || '—'})` : '—';
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Utókeltető kocsi</div><div class="value">${item.trolleyId}</div></div>
          <div><div class="label">Hatcher</div><div class="value">${item.hatcherMachine} · ${item.displaySlotLabel || '—'}</div></div>
          <div><div class="label">Transzfer</div><div class="value">${formatDateTime(item.transferDate)}</div></div>
          <div><div class="label">Candling</div><div class="value">${formatDateTime(item.candlingDate)}</div></div>
          <div><div class="label">Operator</div><div class="value">${item.operator}</div></div>
          <div><div class="label">Termékenység</div><div class="value">${item.fertilityPercent.toFixed(1)}%</div></div>
        </div>
        <div class="slot-summary" style="margin-top:-6px">
          <div><div class="label">Szülőpár telep</div><div class="value">${breederSite}</div></div>
          <div><div class="label">Istálló</div><div class="value">${barn}</div></div>
          <div><div class="label">Szülőpár kora</div><div class="value">${flockAge}</div></div>
          <div><div class="label">Farmkocsik érkezése</div><div class="value">${farmArrivalRange}</div></div>
        </div>
        <div class="slot-batches" style="margin-top:12px">
          <table>
            <thead>
              <tr><th>Kategória</th><th>Db</th><th>%</th></tr>
            </thead>
            <tbody>
              <tr><td>Termékeny (zöld)</td><td>${numberFormatter.format(item.counts.fertile)}</td><td>${percent(item.counts.fertile)}</td></tr>
              <tr><td>Terméketlen (fehér)</td><td>${numberFormatter.format(item.counts.infertile)}</td><td>${percent(item.counts.infertile)}</td></tr>
              <tr><td>Korai elhalt (sárga)</td><td>${numberFormatter.format(item.counts.earlyDead)}</td><td>${percent(item.counts.earlyDead)}</td></tr>
              <tr><td>Kései elhalt (piros)</td><td>${numberFormatter.format(item.counts.lateDead)}</td><td>${percent(item.counts.lateDead)}</td></tr>
              <tr><td>Repedt / Selejt</td><td>${numberFormatter.format(item.counts.cracked || 0)}</td><td>${percent(item.counts.cracked || 0)}</td></tr>
            </tbody>
          </table>
        </div>
      `;

      let vaccineRows = [];
      if (item.vaccines && item.vaccines.length){
        const vacc = document.createElement('div');
        vacc.className = 'vacc-table';
        vacc.innerHTML = `
          <table>
            <thead><tr><th>Vakcina</th><th>Lejárat</th><th>Szállítólevél</th><th>Gyártási szám</th><th>Mennyiség</th></tr></thead>
            <tbody>
              ${item.vaccines.map((v, idx) => `
                <tr>
                  <td><button type="button" class="link-button" data-vaccine-index="${idx}" data-vaccine-product="${v.product}" data-vaccine-type="${v.type || 'in-ovo'}">${v.product}</button></td>
                  <td>${formatDate(v.expiry)}</td>
                  <td>${v.deliveryNote || '—'}</td>
                  <td>${v.lot || '—'}</td>
                  <td>${numberFormatter.format(v.quantity || 0)} db</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        `;
        vaccineRows = Array.from(item.vaccines);
        body.appendChild(vacc);
      }

      const timeline = document.createElement('ul');
      timeline.className = 'slot-timeline';
      timeline.innerHTML = `
        <li><span>Előkeltető pozíció</span><span>${pre ? prePosition : '—'}</span></li>
        <li><span>Állomány</span><span>${pre ? pre.flockName || '—' : '—'}</span></li>
        <li><span>Farmkocsik</span><span>${pre && pre.farmCarts.length ? pre.farmCarts.map((cart) => `${cart.id} (${cart.batchId || 'batch nélkül'})`).join(', ') : '—'}</span></li>
        <li><span>Farm beérkezés</span><span>${farmArrivalRange}</span></li>
        <li><span>Előkeltető berakás</span><span>${prePlacement}</span></li>
        <li><span>Tervezett transzfer</span><span>${pre ? formatDateTime(pre.plannedTransferDate) : '—'}</span></li>
        <li><span>UK slot</span><span>${item.ukSlotLabel || item.plannedUkSlotLabel || 'Tervezés alatt'}</span></li>
        <li><span>Utókeltető transzfer</span><span>${formatDateTime(item.transferDate)}</span></li>
        <li><span>Vakcina beadva</span><span>${item.vaccineWindow ? formatDateTime(item.vaccineWindow) : '—'} (${item.vaccineOperator || '—'})</span></li>
      `;
      body.appendChild(timeline);

      if (item.remarks){
        const note = document.createElement('p');
        note.style.marginTop = '12px';
        note.style.fontSize = '13px';
        note.style.color = 'var(--muted)';
        note.textContent = `Megjegyzés: ${item.remarks}`;
        body.appendChild(note);
      }

      const dialog = createDialog({ title: `Transzfer – ${item.trolleyId}`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] });

      body.querySelectorAll('[data-vaccine-product]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const product = btn.getAttribute('data-vaccine-product');
          const type = btn.getAttribute('data-vaccine-type') || 'in-ovo';
          pendingVaccineFilter = { products: [product], type };
          dialog.close();
          window.location.hash = '#/vaccines';
        });
      });

      dialog.open();
      pendingTransferFilter = null;
    }

    return { el };
  }

  function ChickStoragePage(context){
    const el = document.createElement('div');
    const filterBar = document.createElement('div');
    const meta = document.createElement('div');
    const grid = document.createElement('div');
    filterBar.className = 'filter-bar';
    filterBar.innerHTML = `
      <label>Dátum -tól<input type="date" data-filter="from" /></label>
      <label>Dátum -ig<input type="date" data-filter="to" /></label>
      <button type="button" data-filter="clear">Szűrő törlése</button>
    `;
    meta.className = 'trolley-meta';
    grid.className = 'trolley-grid';
    el.append(filterBar, meta, grid);

    const fromInput = filterBar.querySelector('[data-filter="from"]');
    const toInput = filterBar.querySelector('[data-filter="to"]');
    const clearBtn = filterBar.querySelector('[data-filter="clear"]');

    let filterState = { from: '', to: '' };
    let searchValue = '';
    let searchDisplayValue = '';
    let cards = [];
    let visibleData = [];

    fromInput.addEventListener('change', () => { filterState.from = fromInput.value || ''; render(); });
    toInput.addEventListener('change', () => { filterState.to = toInput.value || ''; render(); });
    clearBtn.addEventListener('click', () => {
      filterState = { from: '', to: '' };
      searchValue = '';
      searchDisplayValue = '';
      const searchInput = document.getElementById('global-search');
      if (searchInput) searchInput.value = '';
      render();
    });

    context.setSearchHandler((term) => { searchValue = term; searchDisplayValue = term; render(); });
    context.setPrimaryAction(() => openSummary());
    render();

    function render(){
      const data = getChickTrolleys();
      const range = computeTransferRange(data);
      if (!filterState.from && range.from) filterState.from = range.from;
      if (!filterState.to && range.to) filterState.to = range.to;
      const filtered = applyDataFilters(data);
      visibleData = filtered;
      renderMeta(filtered);
      renderGrid(filtered);
      if (fromInput) fromInput.value = filterState.from;
      if (toInput) toInput.value = filterState.to;
      const searchInput = document.getElementById('global-search');
      if (searchInput && document.activeElement !== searchInput) {
        const display = searchDisplayValue || searchValue;
        if (display !== undefined) searchInput.value = display;
      }
    }

    function renderMeta(data){
      const totals = data.reduce((acc, item) => {
        acc.boxes += item.boxes || 0;
        acc.chicks += item.chicks || 0;
        acc.full += item.full ? 1 : 0;
        return acc;
      }, { boxes: 0, chicks: 0, full: 0 });
      const avg = data.length ? (totals.chicks / data.length) : 0;
      meta.innerHTML = `
        <div class="meta-card"><div class="meta-label">Csibéskocsik</div><div class="meta-value">${data.length}</div></div>
        <div class="meta-card"><div class="meta-label">Összes láda</div><div class="meta-value">${numberFormatter.format(totals.boxes)} db</div></div>
        <div class="meta-card"><div class="meta-label">Összes csibe</div><div class="meta-value">${numberFormatter.format(totals.chicks)} db</div></div>
        <div class="meta-card"><div class="meta-label">Teljes kocsi</div><div class="meta-value">${totals.full}</div></div>
        <div class="meta-card"><div class="meta-label">Átlag kocsinként</div><div class="meta-value">${Math.round(avg)}</div></div>
      `;
    }

    function renderGrid(data){
      grid.innerHTML = '';
      cards = [];
      data.forEach((item) => {
        const card = document.createElement('button');
        card.type = 'button';
        const goodBad = item.full ? 'good' : 'warn';
        card.className = `trolley-card ${goodBad}`;

        const uk = item.source;
        const pre = uk && uk.preHatch;
        const breeder = uk && (uk.breederFarm || (pre && (pre.arrivalSite || (pre.farmCarts[0] && pre.farmCarts[0].site)))) || '—';
        const layoutBadge = item.chicksPerBox === 70 ? '<span class="pill warn">32×70</span>' : '';

        card.innerHTML = `
          <div class="trolley-header">
            <h3>${item.trolleyId}</h3>
            <div class="trolley-badges">
              <span class="pill">${uk ? uk.hatcherMachine : '—'}</span>
              <span class="pill">${uk ? (uk.ukSlotLabel || uk.displaySlotLabel || 'UK') : '—'}</span>
              ${layoutBadge}
            </div>
          </div>
          <div class="trolley-body">
            <div><span class="label">Szülőpár telep</span><span class="value">${breeder}</span></div>
            <div><span class="label">Teljes kocsi</span><span class="value">${item.full ? 'Igen' : 'Nem'}</span></div>
            <div><span class="label">Fajta</span><span class="value">${item.breed}</span></div>
            <div><span class="label">Azonosítás</span><span class="value">UK: ${uk ? (uk.ukIdentifier || uk.trolleyId) : '—'} · Csibés: ${item.trolleyId}</span></div>
            <div><span class="label">Beosztás</span><span class="value">32×${item.chicksPerBox} csibe/kocsi</span></div>
            <div><span class="label">Ládák</span><span class="value">${item.boxes} db</span></div>
            <div><span class="label">Naposcsibe</span><span class="value">${numberFormatter.format(item.chicks)} db</span></div>
            <div><span class="label">Dátum</span><span class="value">${formatDate(item.transferDate)}</span></div>
            <div><span class="label">Előkeltető</span><span class="value">${pre ? `#${String(pre.slot).padStart(2, '0')} (${pre.prehatchCartId || '—'})` : '—'}</span></div>
            <div><span class="label">Állomány</span><span class="value">${pre ? (pre.flockName || '—') : '—'}</span></div>
            <div><span class="label">Farmkocsik</span><span class="value">${pre && pre.farmCarts && pre.farmCarts.length ? pre.farmCarts.map((c) => c.id).join(', ') : '—'}</span></div>
            <div><span class="label">Farm beérkezés</span><span class="value">${pre ? buildRange(pre.farmCarts.map((c) => c.arrivalDate), false) : '—'}</span></div>
            <div><span class="label">Előkeltető berakás</span><span class="value">${pre ? formatDateTime(pre.placementDate) : '—'}</span></div>
            <div><span class="label">Kiszállítás (terv)</span><span class="value">${formatDate(item.plannedDeliveryDate)}</span></div>
            <div><span class="label">Kiszállítás (tény)</span><span class="value">${formatDate(item.actualDeliveryDate)}</span></div>
          </div>
          <div class="trolley-footer">
            <span>UK: ${uk ? uk.trolleyId : '—'}</span>
            <span>${pre ? (pre.prehatchCartId || '—') : '—'}</span>
          </div>
        `;

        card.addEventListener('click', () => showDetails(item));
        grid.appendChild(card);
        cards.push({ card, data: item });
      });
    }

    function matchesQuery(item, q){
      const uk = item.source || {};
      const pre = uk.preHatch || {};
      const haystack = [
        item.trolleyId,
        String(item.boxes),
        numberFormatter.format(item.chicks),
        String(item.chicksPerBox),
        uk.trolleyId,
        uk.hatcherMachine,
        uk.displaySlotLabel,
        uk.ukIdentifier,
        pre.prehatchCartId,
        pre.flockName,
        pre.arrivalSite,
        ...(pre.farmCarts || []).flatMap((c) => [c.id, c.batchId, c.waybill, c.site])
      ].filter(Boolean).join(' ').toLowerCase();
      return haystack.includes(q);
    }

    function applyDataFilters(items){
      const fromDate = filterState.from ? new Date(filterState.from) : null;
      const toDate = filterState.to ? new Date(filterState.to) : null;
      if (toDate) toDate.setHours(23, 59, 59, 999);
      const q = searchValue.trim().toLowerCase();
      return items.filter((item) => {
        const dt = item.transferDate ? new Date(item.transferDate) : null;
        if (fromDate && (!dt || dt < fromDate)) return false;
        if (toDate && (!dt || dt > toDate)) return false;
        if (!q) return true;
        return matchesQuery(item, q);
      });
    }

    function openSummary(){
      const data = visibleData;
      const totals = data.reduce((acc, it) => {
        acc.trolleys += 1;
        acc.boxes += it.boxes || 0;
        acc.chicks += it.chicks || 0;
        acc.full += it.full ? 1 : 0;
        return acc;
      }, { trolleys: 0, boxes: 0, chicks: 0, full: 0 });
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Kocsik</div><div class="value">${totals.trolleys}</div></div>
          <div><div class="label">Ládák</div><div class="value">${numberFormatter.format(totals.boxes)} db</div></div>
          <div><div class="label">Csibék</div><div class="value">${numberFormatter.format(totals.chicks)} db</div></div>
          <div><div class="label">Teljes</div><div class="value">${totals.full}</div></div>
        </div>
      `;
      createDialog({ title: 'Naposcsibe tárolás – összesítés', body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

    function showDetails(item){
      const uk = item.source;
      const pre = uk && uk.preHatch;
      const breederSite = uk ? (uk.breederFarm || (pre && (pre.arrivalSite || (pre.farmCarts[0] && pre.farmCarts[0].site)))) : '—';
      const body = document.createElement('div');
      body.innerHTML = `
        <div class="slot-summary">
          <div><div class="label">Csibés kocsi</div><div class="value">${item.trolleyId}</div></div>
          <div><div class="label">Ládák</div><div class="value">${item.boxes} × ${item.chicksPerBox} = ${numberFormatter.format(item.chicks)} db</div></div>
          <div><div class="label">Teljes</div><div class="value">${item.full ? 'Igen' : 'Nem'}</div></div>
          <div><div class="label">Dátum</div><div class="value">${formatDateTime(item.transferDate)}</div></div>
        </div>
        <div class="slot-summary" style="margin-top:-6px">
          <div><div class="label">Szülőpár telep</div><div class="value">${breederSite || '—'}</div></div>
          <div><div class="label">Fajta</div><div class="value">${item.breed}</div></div>
          <div><div class="label">UK kocsi</div><div class="value">${uk ? uk.trolleyId : '—'} (${uk ? (uk.hatcherMachine + ' · ' + (uk.ukSlotLabel || uk.displaySlotLabel || '—')) : '—'})</div></div>
        </div>
      `;

      const timeline = document.createElement('ul');
      timeline.className = 'slot-timeline';
      timeline.innerHTML = `
        <li><span>UK azonosító</span><span>${uk ? (uk.ukIdentifier || '—') : '—'}</span></li>
        <li><span>Beosztás</span><span>32×${item.chicksPerBox} csibe/kocsi</span></li>
        <li><span>Transzfer / Candling</span><span>${uk ? `${formatDate(uk.transferDate)} / ${formatDate(uk.candlingDate)}` : '—'}</span></li>
        <li><span>Előkeltető</span><span>${pre ? `#${String(pre.slot).padStart(2,'0')} (${pre.prehatchCartId || '—'})` : '—'}</span></li>
        <li><span>Állomány</span><span>${pre ? pre.flockName || '—' : '—'}</span></li>
        <li><span>Farmkocsik</span><span>${pre && pre.farmCarts && pre.farmCarts.length ? pre.farmCarts.map((c) => `${c.id} · ${c.waybill || '—'}`).join(', ') : '—'}</span></li>
        <li><span>Farm beérkezés</span><span>${pre ? buildRange(pre.farmCarts.map((c) => c.arrivalDate), false) : '—'}</span></li>
        <li><span>Előkeltető berakás</span><span>${pre ? formatDateTime(pre.placementDate) : '—'}</span></li>
      `;
      body.appendChild(timeline);

      createDialog({ title: `Naposcsibe – ${item.trolleyId}`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }

    return { el };
  }

  // --------- Chick Delivery (Kiszállítás) ---------
  const chickDeliverySeed = [
    {
      id: 'DEL-2024-10-08-01',
      date: '2024-10-08',
      hatcheryName: 'PETNEHÁZA KELTETŐ',
      hatcheryAddress: '4542 Petneháza, Hrsz. 095/9.',
      hatcheryQSLocation: '34800000028225456',
      hatcheryQSId: '4953113810185',
      hatcheryIdNumber: '8225456',
      destination: 'Sárospatak',
      destinationFarmId: '4490717',
      destinationQSLocation: '348000004490717',
      totalChicks: 84600,
      batches: [
        {
          chicks: 84600,
          breederFarm: 'Aranyosapáti',
          breed: 'Ross 308',
          qsLocation: '348000006975775',
          breederFarmId: '6975775',
          breederAgeWeeks: 42,
          traceabilityCode: 'LC/MAW-20241008-A',
          houseSlaughterIds: '1,2,3,4,8,12'
        }
      ],
      vaccines: ['Hatchpak Avinew', 'Cevac Mass L', 'Cevac IBird'],
      loadFrom: '08:57',
      loadTo: '10:40',
      ggn: '4050373294984',
      signature: 'STB XY',
      birWaybill: '29412897',
      expectedTransportMinutes: 90,
      healthCertNumber: 'BD986694',
      healthCertIssueDate: '2024-10-08',
      vetStampNumber: 'MOK-147852',
      driverName: 'Kiss Péter',
      vehiclePlate: 'ABC-123',
      issueSlipNumber: 'KK03/0002042'
    },
    {
      id: 'DEL-2024-10-09-01',
      date: '2024-10-09',
      hatcheryName: 'PETNEHÁZA KELTETŐ',
      hatcheryAddress: '4542 Petneháza, Hrsz. 095/9.',
      hatcheryQSLocation: '34800000028225456',
      hatcheryQSId: '4953113810185',
      hatcheryIdNumber: '8225456',
      destination: 'Nyíregyháza',
      destinationFarmId: '4488021',
      destinationQSLocation: '348000004488021',
      totalChicks: 62400,
      batches: [
        {
          chicks: 31200,
          breederFarm: 'Barabás 05',
          breed: 'Ross 308',
          qsLocation: '348000006975771',
          breederFarmId: '6975771',
          breederAgeWeeks: 41,
          traceabilityCode: 'LC/MAW-20241009-B1',
          houseSlaughterIds: '2,4,6,8'
        },
        {
          chicks: 31200,
          breederFarm: 'Ibrány 07',
          breed: 'Ross 308',
          qsLocation: '348000006975772',
          breederFarmId: '6975772',
          breederAgeWeeks: 43,
          traceabilityCode: 'LC/MAW-20241009-B2',
          houseSlaughterIds: '1,3,5,7'
        }
      ],
      vaccines: ['Cevac Mass L', 'Cevac IBird'],
      loadFrom: '07:40',
      loadTo: '09:05',
      ggn: '4050373294984',
      signature: 'Tóth János',
      birWaybill: '29412975',
      expectedTransportMinutes: 85,
      healthCertNumber: 'BD986702',
      healthCertIssueDate: '2024-10-09',
      vetStampNumber: 'MOK-258369',
      driverName: 'Nagy Ádám',
      vehiclePlate: 'DEF-456',
      issueSlipNumber: 'KK03/0002043'
    },
    {
      id: 'DEL-2024-10-10-02',
      date: '2024-10-10',
      hatcheryName: 'PETNEHÁZA KELTETŐ',
      hatcheryAddress: '4542 Petneháza, Hrsz. 095/9.',
      hatcheryQSLocation: '34800000028225456',
      hatcheryQSId: '4953113810185',
      hatcheryIdNumber: '8225456',
      destination: 'Mátészalka',
      destinationFarmId: '4491028',
      destinationQSLocation: '348000004491028',
      totalChicks: 70200,
      batches: [
        {
          chicks: 35100,
          breederFarm: 'Nyírkarász 03',
          breed: 'Ross 308',
          qsLocation: '348000006975780',
          breederFarmId: '6975780',
          breederAgeWeeks: 40,
          traceabilityCode: 'LC/MAW-20241010-C1',
          houseSlaughterIds: '4,9,12'
        },
        {
          chicks: 35100,
          breederFarm: 'Tiszabercel 02',
          breed: 'Ross 308',
          qsLocation: '348000006975781',
          breederFarmId: '6975781',
          breederAgeWeeks: 41,
          traceabilityCode: 'LC/MAW-20241010-C2',
          houseSlaughterIds: '3,5,11'
        }
      ],
      vaccines: ['Hatchpak Avinew'],
      loadFrom: '06:55',
      loadTo: '08:20',
      ggn: '4050373294984',
      signature: 'Kovács M.',
      birWaybill: '29413012',
      expectedTransportMinutes: 110,
      healthCertNumber: 'BD986713',
      healthCertIssueDate: '2024-10-10',
      vetStampNumber: 'MOK-369258',
      driverName: 'Tóth Anna',
      vehiclePlate: 'GHI-789',
      issueSlipNumber: 'KK03/0002044'
    }
  ];

  let chickDeliveryRows = [...chickDeliverySeed];

  async function getChickDeliveryList(){
    await delay(60);
    return [...chickDeliveryRows];
  }

  function ChickDeliveryPage(context){
    const el = document.createElement('div');

    const columns = [
      { key: 'date', label: 'Dátum', sortable: true },
      { key: 'destination', label: 'Rendeltetési hely', sortable: true },
      { key: 'destinationFarmId', label: 'Tenyészetkód', sortable: true },
      { key: 'destinationQSLocation', label: 'QS Location', sortable: true },
      { key: 'birWaybill', label: 'BIR szállítólevél', sortable: true },
      { key: 'issueSlipNumber', label: 'Raktárbizonylat (kiadás)', sortable: true },
      { key: 'vehiclePlate', label: 'Rendszám', sortable: true },
      { key: 'totalChicks', label: 'Összes naposcsibe', sortable: true, render: (row) => numberFormatter.format(row.totalChicks) },
      { key: 'vaccines', label: 'Vakcinák', sortable: false, render: (row) => (row.vaccines || []).join(', ') },
      { key: 'loadWindow', label: 'Rakodás', sortable: false, render: (row) => `${row.loadFrom}–${row.loadTo}` }
    ];

    const table = DataTable({
      columns,
      getData: getChickDeliveryList,
      onView: (row) => showDetails(row)
    });

    el.appendChild(table.el);

    context.setSearchHandler((term) => table.setSearch(term));
    context.setPrimaryAction(() => createDialog({ title: 'Új kiszállítás', body: 'Mock adat – most nem szerkeszthető.' }).open());

    return { el };

    function showDetails(row){
      const body = document.createElement('div');
      const header = document.createElement('div');
      header.className = 'slot-summary';
      header.innerHTML = `
        <div><div class="label">Keltető</div><div class="value">${row.hatcheryName}</div></div>
        <div><div class="label">Cím</div><div class="value">${row.hatcheryAddress}</div></div>
        <div><div class="label">QS Location</div><div class="value">${row.hatcheryQSLocation}</div></div>
        <div><div class="label">QS azonosító</div><div class="value">${row.hatcheryQSId}</div></div>
        <div><div class="label">Keltető ID</div><div class="value">${row.hatcheryIdNumber}</div></div>
      `;
      body.appendChild(header);

      const dest = document.createElement('div');
      dest.className = 'slot-summary';
      dest.innerHTML = `
        <div><div class="label">Rendeltetési hely</div><div class="value">${row.destination}</div></div>
        <div><div class="label">Tenyészetkód</div><div class="value">${row.destinationFarmId}</div></div>
        <div><div class="label">QS Location</div><div class="value">${row.destinationQSLocation}</div></div>
        <div><div class="label">Összes naposcsibe</div><div class="value">${numberFormatter.format(row.totalChicks)} db</div></div>
      `;
      body.appendChild(dest);

      if (row.batches && row.batches.length){
        const tableWrap = document.createElement('div');
        tableWrap.className = 'vacc-table';
        const tbl = document.createElement('table');
        tbl.innerHTML = `
          <thead>
            <tr>
              <th>Naposcsibe (db)</th>
              <th>Szülőpár telep</th>
              <th>Fajta</th>
              <th>QS Location</th>
              <th>Tenyészetkód</th>
              <th>Állomány életkor (hét)</th>
              <th>Nyomonkövetési kód</th>
              <th>Ól/vágóhídi azonosítók</th>
            </tr>
          </thead>
          <tbody>
            ${row.batches.map((b) => `
              <tr>
                <td>${numberFormatter.format(b.chicks)}</td>
                <td>${b.breederFarm}</td>
                <td>${b.breed}</td>
                <td>${b.qsLocation}</td>
                <td>${b.breederFarmId}</td>
                <td>${b.breederAgeWeeks}</td>
                <td>${b.traceabilityCode || '—'}</td>
                <td>${b.houseSlaughterIds || '—'}</td>
              </tr>
            `).join('')}
          </tbody>
        `;
        tableWrap.appendChild(tbl);
        body.appendChild(tableWrap);
      }

      const vacc = document.createElement('div');
      vacc.className = 'slot-summary';
      vacc.innerHTML = `
        <div><div class="label">Vakcinázás</div><div class="value">${(row.vaccines || []).join(', ') || '—'}</div></div>
        <div><div class="label">Rakodás</div><div class="value">${row.loadFrom}–${row.loadTo}</div></div>
        <div><div class="label">GGN</div><div class="value">${row.ggn || '—'}</div></div>
        <div><div class="label">Dátum</div><div class="value">${formatDate(row.date)}</div></div>
        <div><div class="label">Aláírás</div><div class="value">${row.signature || '—'}</div></div>
      `;
      body.appendChild(vacc);

      const transport = document.createElement('div');
      transport.className = 'slot-summary';
      transport.innerHTML = `
        <div><div class="label">BIR szállítólevél</div><div class="value">${row.birWaybill || '—'}</div></div>
        <div><div class="label">Kiadási raktárbizonylat</div><div class="value">${row.issueSlipNumber || '—'}</div></div>
        <div><div class="label">Várható időtartam</div><div class="value">${row.expectedTransportMinutes != null ? row.expectedTransportMinutes + ' perc' : '—'}</div></div>
        <div><div class="label">ÁEÜ biz. sorszám</div><div class="value">${row.healthCertNumber || '—'}</div></div>
        <div><div class="label">ÁEÜ biz. kiállítás</div><div class="value">${row.healthCertIssueDate ? formatDate(row.healthCertIssueDate) : '—'}</div></div>
        <div><div class="label">Állatorvosi kamarai bélyegző</div><div class="value">${row.vetStampNumber || '—'}</div></div>
        <div><div class="label">Gépkocsivezető</div><div class="value">${row.driverName || '—'}</div></div>
        <div><div class="label">Rendszám</div><div class="value">${row.vehiclePlate || '—'}</div></div>
      `;
      body.appendChild(transport);

      createDialog({ title: `Kiszállítás – ${row.destination} (${formatDate(row.date)})`, body, actions: [{ label: 'Bezár', onClick: (close) => close() }] }).open();
    }
  }

  function VaccineInventoryPage(context){
    const el = document.createElement('div');
    const filterBar = document.createElement('div');
    const chips = document.createElement('div');
    const summary = document.createElement('div');
    const sectionsWrap = document.createElement('div');

    filterBar.className = 'filter-bar';
    chips.className = 'chip-list';
    summary.className = 'trolley-meta';
    sectionsWrap.style.display = 'flex';
    sectionsWrap.style.flexDirection = 'column';
    sectionsWrap.style.gap = '16px';

    filterBar.innerHTML = `
      <label>Típus
        <select data-filter="type">
          <option value="all">Mind</option>
          <option value="in-ovo">In ovo</option>
          <option value="day-old">Naposcsibe</option>
        </select>
      </label>
      <button type="button" data-filter="clear">Szűrők törlése</button>
    `;

    const typeSelect = filterBar.querySelector('[data-filter="type"]');
    const clearBtn = filterBar.querySelector('[data-filter="clear"]');

    const inventory = getVaccineInventory();
    let filterState = { type: 'all', products: new Set(), search: '' };

    if (pendingVaccineFilter) {
      if (pendingVaccineFilter.type) {
        filterState.type = pendingVaccineFilter.type;
      }
      if (Array.isArray(pendingVaccineFilter.products)) {
        pendingVaccineFilter.products.forEach((p) => filterState.products.add(p));
      }
      pendingVaccineFilter = null;
    }

    typeSelect.value = filterState.type;

    typeSelect.addEventListener('change', () => {
      filterState.type = typeSelect.value;
      render();
    });

    clearBtn.addEventListener('click', () => {
      filterState = { type: 'all', products: new Set(), search: '' };
      typeSelect.value = 'all';
      const searchInput = document.getElementById('global-search');
      if (searchInput) searchInput.value = '';
      render();
    });

    context.setSearchHandler((term) => {
      filterState.search = term.trim().toLowerCase();
      render();
    });

    el.append(filterBar, chips, summary, sectionsWrap);
    render();

    return { el };

    function render(){
      renderChips();
      const rows = applyFilters(inventory);
      renderSummary(rows);
      renderTables(rows);
    }

    function renderChips(){
      chips.innerHTML = '';
      filterState.products.forEach((product) => {
        const chip = document.createElement('span');
        chip.className = 'chip';
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.textContent = '×';
        btn.addEventListener('click', () => {
          filterState.products.delete(product);
          render();
        });
        chip.textContent = product;
        chip.appendChild(btn);
        chips.appendChild(chip);
      });
    }

    function renderSummary(rows){
      const totals = rows.reduce((acc, item) => {
        acc.total += item.totalQuantity || 0;
        acc.used += item.usedQuantity || 0;
        acc.available += item.availableQuantity || 0;
        return acc;
      }, { total: 0, used: 0, available: 0 });
      summary.innerHTML = `
        <div class="meta-card"><div class="meta-label">Tételek</div><div class="meta-value">${rows.length}</div></div>
        <div class="meta-card"><div class="meta-label">Teljes készlet</div><div class="meta-value">${numberFormatter.format(totals.total)} db</div></div>
        <div class="meta-card"><div class="meta-label">Felhasznált</div><div class="meta-value">${numberFormatter.format(totals.used)} db</div></div>
        <div class="meta-card"><div class="meta-label">Elérhető</div><div class="meta-value">${numberFormatter.format(totals.available)} db</div></div>
      `;
    }

    function renderTables(rows){
      sectionsWrap.innerHTML = '';
      const groups = rows.reduce((acc, row) => {
        (acc[row.type] = acc[row.type] || []).push(row);
        return acc;
      }, {});
      ['in-ovo', 'day-old'].forEach((type) => {
        const sectionRows = groups[type] || [];
        if (!sectionRows.length) return;
        const section = document.createElement('section');
        const heading = document.createElement('h3');
        heading.textContent = type === 'in-ovo' ? 'In ovo vakcinák' : 'Naposcsibe vakcinák';
        heading.style.margin = '6px 0';
        heading.style.fontSize = '16px';
        section.appendChild(heading);

        const tableWrap = document.createElement('div');
        tableWrap.className = 'vacc-table';
        const table = document.createElement('table');
        table.innerHTML = `
          <thead>
            <tr>
              <th>Vakcina megnevezése</th>
              <th>Gyártási szám</th>
              <th>Lejárat</th>
              <th>Szállítólevél</th>
              <th>Felhasznált mennyiség</th>
              <th>Készleten</th>
              <th>Összes készlet</th>
              <th>Várakozási idő (nap)</th>
              <th>Megjegyzés</th>
            </tr>
          </thead>
          <tbody></tbody>
        `;
        const tbody = table.querySelector('tbody');
        sectionRows.forEach((row) => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${row.product}</td>
            <td>${row.batch}</td>
            <td>${formatDate(row.expiry)}</td>
            <td>${row.deliveryNote || '—'}</td>
            <td>${numberFormatter.format(row.usedQuantity || 0)} db</td>
            <td>${numberFormatter.format(row.availableQuantity || 0)} db</td>
            <td>${numberFormatter.format(row.totalQuantity || 0)} db</td>
            <td>${row.withdrawalDays != null ? row.withdrawalDays : '—'}</td>
            <td>${row.remarks || '—'}</td>
          `;
          tbody.appendChild(tr);
        });
        tableWrap.appendChild(table);
        section.appendChild(tableWrap);
        sectionsWrap.appendChild(section);
      });

      if (!sectionsWrap.children.length) {
        const empty = document.createElement('div');
        empty.style.color = 'var(--muted)';
        empty.style.fontSize = '13px';
        empty.textContent = 'Nincs a szűrőkre illeszkedő vakcinatétel.';
        sectionsWrap.appendChild(empty);
      }
    }

    function applyFilters(items){
      return items.filter((item) => {
        if (filterState.type !== 'all' && item.type !== filterState.type) return false;
        if (filterState.products.size && !filterState.products.has(item.product)) return false;
        if (filterState.search) {
          const haystack = [
            item.product,
            item.batch,
            item.deliveryNote,
            item.manufacturer,
            item.remarks,
            item.type
          ].filter(Boolean).join(' ').toLowerCase();
          if (!haystack.includes(filterState.search)) return false;
        }
        return true;
      });
    }
  }
  function computeTransferRange(items){
    let min = null;
    let max = null;
    items.forEach((item) => {
      if (!item.transferDate) return;
      const ts = new Date(item.transferDate);
      if (Number.isNaN(ts.getTime())) return;
      if (!min || ts < min) min = ts;
      if (!max || ts > max) max = ts;
    });
    const toDateInput = (date) => date ? new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 10) : '';
    return { from: toDateInput(min), to: toDateInput(max) };
  }

  const routes = {
    '#/eggs/intake': { title: 'Tojás átvétel', load: (ctx) => EggsIntakePage(ctx) },
    '#/egg-storage': { title: 'Tojásraktár', load: (ctx) => EggStoragePage(ctx) },
    '#/eggs/transfer': { title: 'Tojás átrakás', load: (ctx) => EggTransferPage(ctx) },
    '#/pre-hatch': { title: 'Előkeltetés', load: (ctx) => PreHatchPage(ctx) },
    '#/hatcher/transfer': { title: 'Transzfer', load: (ctx) => HatcherTransferPage(ctx) },
    '#/post-hatch': { title: 'Utókeltetés', load: (ctx) => PostHatchPage(ctx) },
    '#/leszedes': { title: 'Leszedés', load: (ctx) => PullingPage(ctx) },
    '#/chick/storage': { title: 'Naposcsibe tárolás', load: (ctx) => ChickStoragePage(ctx) },
    '#/chick/delivery': { title: 'Kiszállítás', load: (ctx) => ChickDeliveryPage(ctx) },
    '#/vaccines': { title: 'Vakcinák', load: (ctx) => VaccineInventoryPage(ctx) },
    '#/eggs/allocations': { title: 'Szállítmány allokáció', load: () => PlaceholderPage('Szállítmány allokáció') },
    '#/hatchery/batches': { title: 'Keltetés batch-ek', load: () => PlaceholderPage('Keltetés batch-ek') },
    '#/analytics': { title: 'Interaktív analítika', load: (ctx) => AnalyticsPage(ctx) }
  };

  const defaultRoute = '#/eggs/intake';

  function navigate(){
    const hash = window.location.hash || defaultRoute;
    const route = routes[hash] || routes[defaultRoute];
    const main = document.querySelector('.main');
    const ctx = { setSearchHandler, setPrimaryAction };

    setSearchHandler(null);
    setPrimaryAction(null);

    main.innerHTML = '';
    const page = route.load(ctx);
    main.appendChild(page.el);
    const titleEl = document.querySelector('.page-title');
    if (titleEl) titleEl.textContent = route.title;
    setActiveRoute(hash);
  }

  function init(){
    const root = document.getElementById('app');
    if (!root) return;
    renderAppShell(root);
    navigate();
    window.addEventListener('hashchange', navigate);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
