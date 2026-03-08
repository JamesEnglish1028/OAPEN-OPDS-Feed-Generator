(function () {
  const repositorySelect = document.getElementById("repository-select");
  const topLimitInput = document.getElementById("top-limit");
  const refreshButton = document.getElementById("refresh-stats");
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

  function card(title, value) {
    const node = document.createElement("div");
    node.className = "card";
    const h2 = document.createElement("h2");
    h2.textContent = title;
    const p = document.createElement("p");
    p.className = "summary";
    p.textContent = value;
    node.appendChild(h2);
    node.appendChild(p);
    return node;
  }

  function renderSummary(stats) {
    summaryCards.innerHTML = "";
    const totals = stats.totals || {};
    const yearRange = totals.year_range || {};
    const yearLabel = yearRange.min && yearRange.max ? String(yearRange.min) + " - " + String(yearRange.max) : "n/a";
    const values = [
      ["Publications", number(totals.publications)],
      ["Publication Types", number((stats.by_publication_type || []).length)],
      ["Formats", number((stats.by_format || []).length)],
      ["Languages", number(totals.languages)],
      ["Publishers", number(totals.publishers)],
      ["Unique Authors", number(totals.unique_authors)],
      ["Series", number(totals.series)],
      ["Year Range", yearLabel],
    ];
    for (const item of values) {
      summaryCards.appendChild(card(item[0], item[1]));
    }
  }

  function renderTable(node, columns, rows, emptyLabel) {
    node.innerHTML = "";
    if (!Array.isArray(rows) || rows.length === 0) {
      const empty = document.createElement("p");
      empty.className = "hint";
      empty.textContent = emptyLabel || "No data.";
      node.appendChild(empty);
      return;
    }
    const table = document.createElement("table");
    table.style.width = "100%";
    table.style.borderCollapse = "collapse";
    const thead = document.createElement("thead");
    const trHead = document.createElement("tr");
    for (const column of columns) {
      const th = document.createElement("th");
      th.textContent = column.label;
      th.style.textAlign = "left";
      th.style.padding = "8px 6px";
      th.style.borderBottom = "1px solid var(--line)";
      trHead.appendChild(th);
    }
    thead.appendChild(trHead);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    for (const row of rows) {
      const tr = document.createElement("tr");
      for (const column of columns) {
        const td = document.createElement("td");
        const value = row[column.key];
        td.textContent = column.numeric ? number(value) : String(value == null ? "" : value);
        td.style.padding = "8px 6px";
        td.style.borderBottom = "1px solid var(--line)";
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    node.appendChild(table);
  }

  function renderGroups(groups) {
    renderTable(
      groupsTable,
      [
        { key: "title", label: "Group" },
        { key: "count", label: "Publications", numeric: true },
      ],
      groups.top_level || [],
      "No group counts."
    );

    const subgroupRows = [];
    for (const group of groups.subgroups || []) {
      for (const subgroup of group.subgroups || []) {
        subgroupRows.push({
          group: group.group_title,
          subgroup: subgroup.title,
          count: subgroup.count,
        });
      }
    }
    subgroupRows.sort((a, b) => {
      if (a.group < b.group) return -1;
      if (a.group > b.group) return 1;
      if (a.subgroup < b.subgroup) return -1;
      if (a.subgroup > b.subgroup) return 1;
      return 0;
    });
    renderTable(
      subgroupsTable,
      [
        { key: "group", label: "Group" },
        { key: "subgroup", label: "Subgroup" },
        { key: "count", label: "Publications", numeric: true },
      ],
      subgroupRows,
      "No subgroup counts."
    );
  }

  async function loadRepositories() {
    setStatus("Loading repositories...");
    const response = await fetch("/repositories");
    const data = await readJson(response);
    if (!response.ok) {
      throw new Error(typeof data === "string" ? data : JSON.stringify(data));
    }
    repositorySelect.innerHTML = "";
    for (const repo of data.repositories || []) {
      const option = document.createElement("option");
      option.value = repo.repository_id;
      option.textContent = repo.name + " (" + repo.repository_id + ")";
      repositorySelect.appendChild(option);
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
    setStatus("Loading stats for " + repositoryId + "...");
    const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/stats?top_limit=" + encodeURIComponent(topLimit));
    const data = await readJson(response);
    if (!response.ok) {
      throw new Error(typeof data === "string" ? data : JSON.stringify(data));
    }

    renderSummary(data);
    renderTable(typesTable, [{ key: "type", label: "Type" }, { key: "count", label: "Publications", numeric: true }], data.by_publication_type || [], "No publication type data.");
    renderTable(formatsTable, [{ key: "format", label: "Format (MIME)" }, { key: "count", label: "Publications", numeric: true }], data.by_format || [], "No format data.");
    renderTable(languagesTable, [{ key: "language", label: "Language" }, { key: "count", label: "Publications", numeric: true }], data.by_language || [], "No language data.");
    renderTable(publishersTable, [{ key: "publisher", label: "Publisher" }, { key: "count", label: "Publications", numeric: true }], data.by_publisher || [], "No publisher data.");
    renderTable(authorsTable, [{ key: "name", label: "Author" }, { key: "count", label: "Publications", numeric: true }], (data.authors && data.authors.top_authors) || [], "No author data.");
    renderTable(seriesTable, [{ key: "name", label: "Series" }, { key: "count", label: "Publications", numeric: true }], data.by_series || [], "No series data.");
    renderTable(yearsTable, [{ key: "year", label: "Year" }, { key: "count", label: "Publications", numeric: true }], data.by_publication_year || [], "No publication year data.");
    renderGroups(data.groups || {});

    setStatus("Loaded stats for " + (data.repository_name || repositoryId) + ".");
  }

  async function bootstrap() {
    try {
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

  bootstrap();
})();
