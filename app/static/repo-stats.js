(function () {
  const repositorySelect = document.getElementById("repository-select");
  const topLimitInput = document.getElementById("top-limit");
  const rowLimitInput = document.getElementById("row-limit");
  const sortOrderInput = document.getElementById("sort-order");
  const refreshButton = document.getElementById("refresh-stats");
  const expandAllButton = document.getElementById("expand-all");
  const collapseAllButton = document.getElementById("collapse-all");
  const statusNode = document.getElementById("status");
  const summaryCards = document.getElementById("summary-cards");

  const typesTable = document.getElementById("types-table");
  const formatsTable = document.getElementById("formats-table");
  const languagesTable = document.getElementById("languages-table");
  const publishersTable = document.getElementById("publishers-table");
  const authorsTable = document.getElementById("authors-table");
  const seriesTable = document.getElementById("series-table");
  const yearsTable = document.getElementById("years-table");
  const groupsTable = document.getElementById("groups-table");
  const subgroupsTable = document.getElementById("subgroups-table");

  const authorFilterInput = document.getElementById("author-filter");
  const publisherFilterInput = document.getElementById("publisher-filter");
  const seriesFilterInput = document.getElementById("series-filter");
  const sections = Array.from(document.querySelectorAll("details.stats-section"));

  let latestStats = null;

  function setStatus(message) {
    statusNode.textContent = message;
  }

  async function readJson(response) {
    const contentType = response.headers.get("content-type") || "";
    if (contentType.includes("application/json")) {
      return response.json();
    }
    return response.text();
  }

  function number(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n.toLocaleString() : "0";
  }

  function ratio(value, total) {
    const a = Number(value);
    const b = Number(total);
    if (!Number.isFinite(a) || !Number.isFinite(b) || b <= 0) {
      return "0.0%";
    }
    return ((a / b) * 100).toFixed(1) + "%";
  }

  function currentParams() {
    return new URLSearchParams(window.location.search);
  }

  function writeParams() {
    const params = currentParams();
    if (repositorySelect.value) params.set("repo", repositorySelect.value);
    params.set("limit", String(Math.max(1, Math.min(500, Number(topLimitInput.value) || 25))));
    params.set("rows", String(Math.max(1, Math.min(250, Number(rowLimitInput.value) || 50))));
    params.set("sort", sortOrderInput.value || "count-desc");
    const open = sections.filter((section) => section.open).map((section) => section.getAttribute("data-section"));
    params.set("open", open.join(","));
    const nextUrl = window.location.pathname + "?" + params.toString();
    window.history.replaceState({}, "", nextUrl);
  }

  function applyParams() {
    const params = currentParams();
    const limit = Number(params.get("limit"));
    if (Number.isFinite(limit) && limit >= 1 && limit <= 500) {
      topLimitInput.value = String(limit);
    }
    const rows = Number(params.get("rows"));
    if (Number.isFinite(rows) && rows >= 1 && rows <= 250) {
      rowLimitInput.value = String(rows);
    }
    const sort = params.get("sort");
    if (sort) {
      sortOrderInput.value = sort;
    }
    const open = params.get("open");
    if (open) {
      const selected = new Set(open.split(",").map((item) => item.trim()).filter(Boolean));
      for (const section of sections) {
        section.open = selected.has(section.getAttribute("data-section"));
      }
    }
  }

  function card(title, value, note) {
    const node = document.createElement("div");
    node.className = "card stats-kpi";
    const h2 = document.createElement("h2");
    h2.textContent = title;
    const p = document.createElement("div");
    p.className = "stats-kpi-value";
    p.textContent = value;
    node.appendChild(h2);
    node.appendChild(p);
    if (note) {
      const noteNode = document.createElement("div");
      noteNode.className = "stats-kpi-note";
      noteNode.textContent = note;
      node.appendChild(noteNode);
    }
    return node;
  }

  function sortRows(rows, labelKeys, numericKey) {
    const sortOrder = sortOrderInput.value || "count-desc";
    const items = Array.isArray(rows) ? rows.slice() : [];
    if (!items.length) return items;
    if (sortOrder === "count-asc" || sortOrder === "count-desc") {
      const dir = sortOrder === "count-asc" ? 1 : -1;
      items.sort((a, b) => dir * ((Number(a[numericKey]) || 0) - (Number(b[numericKey]) || 0)));
      return items;
    }
    const dir = sortOrder === "name-desc" ? -1 : 1;
    const key = labelKeys.find((item) => item in items[0]) || labelKeys[0];
    items.sort((a, b) => dir * String(a[key] || "").localeCompare(String(b[key] || "")));
    return items;
  }

  function renderChips(container, rows, labelKey, numericKey) {
    if (!Array.isArray(rows) || !rows.length) return;
    const chipRow = document.createElement("div");
    chipRow.className = "stats-chip-row";
    for (const row of rows.slice(0, 5)) {
      const chip = document.createElement("span");
      chip.className = "stats-chip";
      const valueNode = document.createElement("b");
      valueNode.textContent = number(row[numericKey]);
      chip.appendChild(valueNode);
      chip.appendChild(document.createTextNode(" " + String(row[labelKey] || "")));
      chipRow.appendChild(chip);
    }
    container.appendChild(chipRow);
  }

  function renderTable(node, columns, rows, emptyLabel, options) {
    node.innerHTML = "";
    const settings = options || {};
    const maxRows = Math.max(1, Math.min(250, Number(rowLimitInput.value) || 50));
    let workingRows = Array.isArray(rows) ? rows.slice() : [];

    if (typeof settings.filterText === "string" && settings.filterText.trim()) {
      const needle = settings.filterText.trim().toLowerCase();
      workingRows = workingRows.filter((row) => String(row[settings.filterKey] || "").toLowerCase().includes(needle));
    }

    workingRows = sortRows(workingRows, settings.labelKeys || [columns[0].key], settings.numericKey || columns[1].key);
    if (settings.showTopChips) {
      renderChips(node, workingRows, settings.labelKeys ? settings.labelKeys[0] : columns[0].key, settings.numericKey || columns[1].key);
    }
    workingRows = workingRows.slice(0, maxRows);

    if (!workingRows.length) {
      const empty = document.createElement("p");
      empty.className = "hint";
      empty.textContent = emptyLabel || "No data.";
      node.appendChild(empty);
      return;
    }

    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    const table = document.createElement("table");
    table.className = "stats-table";

    const thead = document.createElement("thead");
    const trHead = document.createElement("tr");
    for (const column of columns) {
      const th = document.createElement("th");
      th.textContent = column.label;
      trHead.appendChild(th);
    }
    if (settings.withBars) {
      const thBar = document.createElement("th");
      thBar.textContent = "Share";
      trHead.appendChild(thBar);
    }
    thead.appendChild(trHead);
    table.appendChild(thead);

    const maxValue = Math.max(...workingRows.map((item) => Number(item[settings.numericKey || columns[1].key]) || 0), 1);
    const tbody = document.createElement("tbody");
    for (const row of workingRows) {
      const tr = document.createElement("tr");
      for (const column of columns) {
        const td = document.createElement("td");
        const value = row[column.key];
        td.textContent = column.numeric ? number(value) : String(value == null ? "" : value);
        if (column.numeric) {
          td.classList.add("numeric");
        }
        tr.appendChild(td);
      }
      if (settings.withBars) {
        const tdBar = document.createElement("td");
        tdBar.className = "bar-cell";
        const track = document.createElement("div");
        track.className = "bar-track";
        const fill = document.createElement("div");
        fill.className = "bar-fill";
        const width = Math.max(0, Math.min(100, ((Number(row[settings.numericKey || columns[1].key]) || 0) / maxValue) * 100));
        fill.style.width = width.toFixed(2) + "%";
        track.appendChild(fill);
        tdBar.appendChild(track);
        tr.appendChild(tdBar);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
    node.appendChild(wrap);
  }

  function renderSummary(stats) {
    summaryCards.innerHTML = "";
    const totals = stats.totals || {};
    const yearRange = totals.year_range || {};
    const publications = Number(totals.publications) || 0;

    const groupedCount = (stats.groups && stats.groups.top_level || []).reduce((sum, row) => sum + (Number(row.count) || 0), 0);
    const withYear = (stats.by_publication_year || []).reduce((sum, row) => sum + (Number(row.count) || 0), 0);
    const withLanguage = (stats.by_language || []).reduce((sum, row) => sum + (Number(row.count) || 0), 0);
    const yearLabel = yearRange.min && yearRange.max ? String(yearRange.min) + " - " + String(yearRange.max) : "n/a";

    const values = [
      ["Publications", number(publications), ""],
      ["Year Range", yearLabel, ""],
      ["Unique Authors", number(totals.unique_authors), "Top listed in People & Imprints"],
      ["Languages", number(totals.languages), ratio(withLanguage, publications) + " of publications have language"],
      ["Publishers", number(totals.publishers), ""],
      ["Series", number(totals.series), ""],
      ["Grouped Coverage", ratio(groupedCount, publications), number(groupedCount) + " publications mapped to top-level groups"],
      ["Year Coverage", ratio(withYear, publications), number(withYear) + " publications have publication year"],
    ];
    for (const item of values) {
      summaryCards.appendChild(card(item[0], item[1], item[2]));
    }
  }

  function renderGroups(groups, totalPublications) {
    const topLevel = (groups.top_level || []).map((row) => {
      const count = Number(row.count) || 0;
      return {
        title: row.title,
        count: count,
        coverage: ratio(count, totalPublications),
      };
    });
    renderTable(
      groupsTable,
      [
        { key: "title", label: "Group" },
        { key: "count", label: "Publications", numeric: true },
        { key: "coverage", label: "Coverage" },
      ],
      topLevel,
      "No group counts.",
      { numericKey: "count", labelKeys: ["title"], withBars: true }
    );

    const subgroupRows = [];
    for (const group of groups.subgroups || []) {
      for (const subgroup of group.subgroups || []) {
        subgroupRows.push({
          group: group.group_title,
          subgroup: subgroup.title,
          count: Number(subgroup.count) || 0,
        });
      }
    }
    renderTable(
      subgroupsTable,
      [
        { key: "group", label: "Group" },
        { key: "subgroup", label: "Subgroup" },
        { key: "count", label: "Publications", numeric: true },
      ],
      subgroupRows,
      "No subgroup counts.",
      { numericKey: "count", labelKeys: ["subgroup", "group"], withBars: true }
    );
  }

  function renderAll(stats) {
    latestStats = stats;
    const totalPublications = Number(stats && stats.totals && stats.totals.publications) || 0;
    renderSummary(stats);
    renderTable(
      typesTable,
      [{ key: "type", label: "Type" }, { key: "count", label: "Publications", numeric: true }],
      stats.by_publication_type || [],
      "No publication type data.",
      { numericKey: "count", labelKeys: ["type"], withBars: true, showTopChips: true }
    );
    renderTable(
      formatsTable,
      [{ key: "format", label: "Format (MIME)" }, { key: "count", label: "Publications", numeric: true }],
      stats.by_format || [],
      "No format data.",
      { numericKey: "count", labelKeys: ["format"], withBars: true, showTopChips: true }
    );
    renderTable(
      languagesTable,
      [{ key: "language", label: "Language" }, { key: "count", label: "Publications", numeric: true }],
      stats.by_language || [],
      "No language data.",
      { numericKey: "count", labelKeys: ["language"], withBars: true }
    );
    renderTable(
      publishersTable,
      [{ key: "publisher", label: "Publisher" }, { key: "count", label: "Publications", numeric: true }],
      stats.by_publisher || [],
      "No publisher data.",
      {
        numericKey: "count",
        labelKeys: ["publisher"],
        withBars: true,
        filterKey: "publisher",
        filterText: publisherFilterInput.value,
      }
    );
    renderTable(
      authorsTable,
      [{ key: "name", label: "Author" }, { key: "count", label: "Publications", numeric: true }],
      (stats.authors && stats.authors.top_authors) || [],
      "No author data.",
      {
        numericKey: "count",
        labelKeys: ["name"],
        withBars: true,
        filterKey: "name",
        filterText: authorFilterInput.value,
      }
    );
    renderTable(
      seriesTable,
      [{ key: "name", label: "Series" }, { key: "count", label: "Publications", numeric: true }],
      stats.by_series || [],
      "No series data.",
      {
        numericKey: "count",
        labelKeys: ["name"],
        withBars: true,
        filterKey: "name",
        filterText: seriesFilterInput.value,
      }
    );
    renderTable(
      yearsTable,
      [{ key: "year", label: "Year" }, { key: "count", label: "Publications", numeric: true }],
      stats.by_publication_year || [],
      "No publication year data.",
      { numericKey: "count", labelKeys: ["year"], withBars: true }
    );
    renderGroups(stats.groups || {}, totalPublications);
  }

  async function loadRepositories() {
    setStatus("Loading repositories...");
    const response = await fetch("/repositories");
    const data = await readJson(response);
    if (!response.ok) {
      throw new Error(typeof data === "string" ? data : JSON.stringify(data));
    }
    const params = currentParams();
    const requestedRepo = params.get("repo");
    repositorySelect.innerHTML = "";
    for (const repo of data.repositories || []) {
      const option = document.createElement("option");
      option.value = repo.repository_id;
      option.textContent = repo.name + " (" + repo.repository_id + ")";
      repositorySelect.appendChild(option);
    }
    if (requestedRepo && Array.from(repositorySelect.options).some((opt) => opt.value === requestedRepo)) {
      repositorySelect.value = requestedRepo;
    }
    setStatus("Repositories loaded.");
  }

  async function loadStats() {
    const repositoryId = repositorySelect.value;
    if (!repositoryId) {
      setStatus("No repositories available.");
      return;
    }
    const topLimit = Math.max(1, Math.min(500, Number(topLimitInput.value) || 25));
    writeParams();
    setStatus("Loading stats for " + repositoryId + "...");
    const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/stats?top_limit=" + encodeURIComponent(topLimit));
    const data = await readJson(response);
    if (!response.ok) {
      throw new Error(typeof data === "string" ? data : JSON.stringify(data));
    }
    renderAll(data);
    setStatus("Loaded stats for " + (data.repository_name || repositoryId) + ".");
  }

  function refreshFromLocalState() {
    if (!latestStats) return;
    renderAll(latestStats);
    writeParams();
  }

  function setAllSections(open) {
    for (const section of sections) {
      section.open = open;
    }
    writeParams();
  }

  async function bootstrap() {
    try {
      applyParams();
      await loadRepositories();
      await loadStats();
    } catch (error) {
      setStatus("Failed: " + String(error));
    }
  }

  refreshButton.addEventListener("click", function () {
    loadStats().catch((error) => setStatus("Failed: " + String(error)));
  });
  repositorySelect.addEventListener("change", function () {
    loadStats().catch((error) => setStatus("Failed: " + String(error)));
  });
  topLimitInput.addEventListener("change", function () {
    loadStats().catch((error) => setStatus("Failed: " + String(error)));
  });
  rowLimitInput.addEventListener("change", refreshFromLocalState);
  sortOrderInput.addEventListener("change", refreshFromLocalState);
  authorFilterInput.addEventListener("input", refreshFromLocalState);
  publisherFilterInput.addEventListener("input", refreshFromLocalState);
  seriesFilterInput.addEventListener("input", refreshFromLocalState);
  expandAllButton.addEventListener("click", function () {
    setAllSections(true);
  });
  collapseAllButton.addEventListener("click", function () {
    setAllSections(false);
  });
  for (const section of sections) {
    section.addEventListener("toggle", writeParams);
  }

  bootstrap();
})();
