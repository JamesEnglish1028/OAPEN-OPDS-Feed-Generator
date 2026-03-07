    const viewPanels = Array.from(document.querySelectorAll(".view-panel"));
    const viewButtons = Array.from(document.querySelectorAll("[data-view-button]"));
    const DEFAULT_REPOSITORY_ID = "default";
    const repoList = document.getElementById("repo-list");
    const repoForm = document.getElementById("repo-form");
    const repoSelect = document.getElementById("harvest-repo");
    const repoSummary = document.getElementById("repo-summary");
    const repoDetail = document.getElementById("repo-detail");
    const repoDetailEmpty = document.getElementById("repo-detail-empty");
    const repoDetailActions = document.getElementById("repo-detail-actions");
    const maintenanceEmpty = document.getElementById("maintenance-empty");
    const maintenanceSelected = document.getElementById("maintenance-selected");
    const maintenanceSelectedName = document.getElementById("maintenance-selected-name");
    const maintenanceRebuildFeeds = document.getElementById("maintenance-rebuild-feeds");
    const maintenanceReindexSubjects = document.getElementById("maintenance-reindex-subjects");
    const maintenanceReindexAuthorities = document.getElementById("maintenance-reindex-authorities");
    const maintenanceInvalidateRepoCache = document.getElementById("maintenance-invalidate-repo-cache");
    const maintenanceInvalidateAllCache = document.getElementById("maintenance-invalidate-all-cache");
    const maintenanceClearData = document.getElementById("maintenance-clear-data");
    const maintenanceDeleteRepository = document.getElementById("maintenance-delete-repository");
    const saveRepoButton = document.getElementById("save-repo");
    const refreshReposButton = document.getElementById("refresh-repos");
    const refreshReposManageButton = document.getElementById("refresh-repos-manage");
    const repoIdInput = document.getElementById("repo-id");
    const repoNameInput = document.getElementById("repo-name");
    const repoTypeInput = document.getElementById("repo-type");
    const repoConfigInput = document.getElementById("repo-config");
    const repoActiveInput = document.getElementById("repo-active");
    const harvestForm = document.getElementById("harvest-form");
    const harvestUrlInput = document.getElementById("harvest-url");
    const harvestMaxPagesInput = document.getElementById("harvest-max-pages");
    const harvestMaxRecordsInput = document.getElementById("harvest-max-records");
    const harvestTimeoutInput = document.getElementById("harvest-timeout");
    const harvestFollowNextInput = document.getElementById("harvest-follow-next");
    const harvestIncrementalInput = document.getElementById("harvest-incremental");
    const directoryModeSplitInput = document.getElementById("directory-mode-split");
    const directoryModeSingleInput = document.getElementById("directory-mode-single");
    const directoryImportSummary = document.getElementById("directory-import-summary");
    const directoryList = document.getElementById("directory-list");
    const directorySelectAll = document.getElementById("directory-select-all");
    const startHarvestButton = document.getElementById("start-harvest");
    const fetchDirectoriesButton = document.getElementById("fetch-directories");
    const loadCheckpointsButton = document.getElementById("load-checkpoints");
    const importAsRepositoriesButton = document.getElementById("import-as-repositories");
    const importIntoRepositoryButton = document.getElementById("import-into-repository");
    const harvestActionHint = document.getElementById("harvest-action-hint");
    const harvestStatus = document.getElementById("harvest-status");
    const harvestStatusText = document.getElementById("harvest-status-text");
    const output = document.getElementById("output");
    const subjectBackfillCursors = {};
    let directoryEntries = [];
    let selectedRepository = null;
    let harvestBusy = false;
    let activeView = "repositories";

    function show(data, options) {
      const settings = options || {};
      const inferredStatus = data && typeof data === "object" && data.error ? "ERROR" : "INFO";
      const status = settings.status || inferredStatus;
      const action = settings.action ? " | " + settings.action : "";
      const rendered = typeof data === "string" ? data : JSON.stringify(data, null, 2);
      output.textContent = "[" + status + action + "]\n" + rendered;
    }

    function showActionResponse(action, response, data) {
      show(data, { action: action, status: response.ok ? "SUCCESS" : "ERROR" });
    }

    function setHarvestResult(ok, label) {
      harvestStatus.classList.remove("running");
      harvestStatus.classList.toggle("success", Boolean(ok));
      harvestStatus.classList.toggle("error", !ok);
      harvestStatusText.textContent = label || (ok ? "Succeeded" : "Failed");
    }

    function setActiveView(viewName) {
      activeView = viewName;
      for (const panel of viewPanels) {
        const panelView = panel.getAttribute("data-view");
        panel.hidden = panelView !== viewName;
      }
      for (const button of viewButtons) {
        const buttonView = button.getAttribute("data-view-button");
        button.classList.toggle("active", buttonView === viewName);
      }
    }

    function resolveRepositoryId(explicitRepositoryId, fallbackValue) {
      const value = explicitRepositoryId || fallbackValue || "";
      return value.trim();
    }

    function updateSubjectBackfillCursor(repositoryId, data) {
      if (!repositoryId || !data) {
        return;
      }
      if (typeof data.next_cursor === "string" && data.next_cursor) {
        subjectBackfillCursors[repositoryId] = data.next_cursor;
        return;
      }
      if (data.has_more === false) {
        subjectBackfillCursors[repositoryId] = "";
      }
    }

    async function refreshRepositoriesSilently(preferredRepositoryId) {
      await loadRepositories(preferredRepositoryId, { silent: true });
    }

    async function readJson(response) {
      const contentType = response.headers.get("content-type") || "";
      if (contentType.includes("application/json")) {
        return response.json();
      }
      return response.text();
    }

    function normalizeConfigText(text) {
      const trimmed = text.trim();
      if (!trimmed) {
        return {};
      }
      try {
        return JSON.parse(trimmed);
      } catch (error) {
        throw new Error("Config JSON is invalid: " + error.message);
      }
    }

    function isConfigJsonValid() {
      try {
        normalizeConfigText(repoConfigInput.value);
        return true;
      } catch (error) {
        return false;
      }
    }

    function updateRepoFormButtonState() {
      const hasId = Boolean(repoIdInput.value.trim());
      const hasName = Boolean(repoNameInput.value.trim());
      const hasValidConfig = isConfigJsonValid();
      saveRepoButton.disabled = !(hasId && hasName && hasValidConfig) || harvestBusy;
    }

    function getCheckedDirectoryEntries() {
      const selected = [];
      for (const input of directoryList.querySelectorAll("input[data-directory-index]")) {
        if (!input.checked) {
          continue;
        }
        const index = Number(input.getAttribute("data-directory-index"));
        if (!Number.isFinite(index) || !directoryEntries[index]) {
          continue;
        }
        selected.push(directoryEntries[index]);
      }
      return selected;
    }

    function currentDirectoryImportMode() {
      const selected = document.querySelector('input[name="directory_import_mode"]:checked');
      if (!selected || typeof selected.value !== "string") {
        return "split-repositories";
      }
      return selected.value;
    }

    function setHarvestBusy(isBusy, label) {
      harvestBusy = isBusy;
      if (isBusy) {
        harvestStatus.classList.remove("success", "error");
        harvestStatus.classList.add("running");
        harvestStatusText.textContent = label || "Running";
      } else {
        harvestStatus.classList.remove("running");
        if (!harvestStatus.classList.contains("success") && !harvestStatus.classList.contains("error")) {
          harvestStatusText.textContent = "Idle";
        }
      }
      updateHarvestActionState();
    }

    function updateDirectoryImportSummary() {
      const selectedEntries = getCheckedDirectoryEntries();
      const selectedCount = selectedEntries.length;
      const mode = currentDirectoryImportMode();
      const targetRepository = repoSelect.value || "(none)";
      if (selectedCount === 0) {
        directoryImportSummary.textContent = "Select one or more directories to import.";
        return;
      }
      if (mode === "split-repositories") {
        directoryImportSummary.textContent = "This will create " + selectedCount + " repository" + (selectedCount === 1 ? "" : "ies") + " from the selected directories.";
        return;
      }
      directoryImportSummary.textContent = "This will import " + selectedCount + " directories into repository '" + targetRepository + "' as collections.";
    }

    function updateHarvestActionState() {
      const url = harvestUrlInput.value.trim();
      const hasUrl = Boolean(url);
      const hasRepository = Boolean(repoSelect.value);
      const selectedCount = getCheckedDirectoryEntries().length;
      const mode = currentDirectoryImportMode();
      refreshReposManageButton.disabled = harvestBusy;
      fetchDirectoriesButton.disabled = harvestBusy || !hasUrl;
      loadCheckpointsButton.disabled = harvestBusy || !hasRepository;
      directorySelectAll.disabled = directoryEntries.length === 0;

      if (!hasUrl) {
        startHarvestButton.disabled = true;
        importAsRepositoriesButton.disabled = true;
        importIntoRepositoryButton.disabled = true;
        if (harvestBusy) {
          harvestActionHint.textContent = "Harvest is running...";
          return;
        }
        harvestActionHint.textContent = "Enter a feed URL to begin.";
        updateRepoFormButtonState();
        return;
      }

      if (!hasRepository) {
        startHarvestButton.disabled = true;
        importAsRepositoriesButton.disabled = true;
        importIntoRepositoryButton.disabled = true;
        if (harvestBusy) {
          harvestActionHint.textContent = "Harvest is running...";
          updateRepoFormButtonState();
          return;
        }
        harvestActionHint.textContent = "Select a repository to run ingest actions.";
        updateRepoFormButtonState();
        return;
      }

      if (selectedCount > 0) {
        startHarvestButton.disabled = true;
        importAsRepositoriesButton.disabled = harvestBusy || !(mode === "split-repositories");
        importIntoRepositoryButton.disabled = harvestBusy || !(mode === "single-repository-collections");
        if (harvestBusy) {
          harvestActionHint.textContent = "Harvest is running...";
          updateRepoFormButtonState();
          return;
        }
        if (mode === "split-repositories") {
          harvestActionHint.textContent = "Directories selected: use 'Create Repositories' to import them.";
        } else {
          harvestActionHint.textContent = "Directories selected: use 'Import Into Selected Repository' to ingest as collections.";
        }
        updateRepoFormButtonState();
        return;
      }

      startHarvestButton.disabled = harvestBusy ? true : false;
      importAsRepositoriesButton.disabled = true;
      importIntoRepositoryButton.disabled = true;
      if (harvestBusy) {
        harvestActionHint.textContent = "Harvest is running...";
        updateRepoFormButtonState();
        return;
      }
      harvestActionHint.textContent = "No directories selected: use 'Start Harvest' for direct single-feed ingest, or fetch/select directories first.";
      updateRepoFormButtonState();
    }

    function syncDirectoryImportModeUi() {
      const mode = currentDirectoryImportMode();
      maybeAutofillSplitRepositoryConfig();
      updateDirectoryImportSummary();
      updateHarvestActionState();
    }

    function renderDirectoryEntries(entries) {
      directoryEntries = Array.isArray(entries) ? entries : [];
      directoryList.innerHTML = "";
      directorySelectAll.checked = false;
      if (!directoryEntries.length) {
        const empty = document.createElement("small");
        empty.textContent = "No directories found yet.";
        empty.style.color = "var(--muted)";
        directoryList.appendChild(empty);
        return;
      }
      directoryEntries.forEach((entry, index) => {
        const row = document.createElement("label");
        row.className = "check";
        row.style.margin = "0";
        const title = typeof entry.title === "string" && entry.title.trim() ? entry.title.trim() : entry.href;
        const group = typeof entry.group === "string" && entry.group.trim() ? " [" + entry.group.trim() + "]" : "";
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.setAttribute("data-directory-index", String(index));

        const textWrap = document.createElement("span");
        const strong = document.createElement("strong");
        strong.textContent = title;
        const groupNode = document.createTextNode(group);
        const lineBreak = document.createElement("br");
        const small = document.createElement("small");
        small.style.color = "var(--muted)";
        small.textContent = entry.href;

        textWrap.appendChild(strong);
        textWrap.appendChild(groupNode);
        textWrap.appendChild(lineBreak);
        textWrap.appendChild(small);
        row.appendChild(checkbox);
        row.appendChild(textWrap);

        checkbox.addEventListener("change", () => {
          maybeAutofillSplitRepositoryConfig();
          updateDirectoryImportSummary();
          updateHarvestActionState();
        });
        directoryList.appendChild(row);
      });
      updateDirectoryImportSummary();
      updateHarvestActionState();
    }

    function maybeAutofillSplitRepositoryConfig() {
      const mode = currentDirectoryImportMode();
      if (mode !== "split-repositories") {
        return;
      }
      const selectedEntries = getCheckedDirectoryEntries();
      if (selectedEntries.length !== 1) {
        return;
      }
      const entry = selectedEntries[0];
      const nextConfig = { url: entry.href };
      const title = typeof entry.title === "string" ? entry.title.trim() : "";
      const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
      repoConfigInput.value = JSON.stringify(nextConfig, null, 2);
      if (slug) {
        repoIdInput.value = slug;
      }
      if (title) {
        repoNameInput.value = title;
      }
    }

    function createDetailRow(label, value) {
      const row = document.createElement("div");
      row.className = "detail-row";
      const labelNode = document.createElement("div");
      labelNode.className = "detail-label";
      labelNode.textContent = label;
      const valueNode = document.createElement("div");
      valueNode.className = "detail-value";
      valueNode.textContent = value;
      row.appendChild(labelNode);
      row.appendChild(valueNode);
      return row;
    }

    function renderRepositoryDetail(repo) {
      selectedRepository = repo || null;
      repoDetail.innerHTML = "";
      repoDetailActions.innerHTML = "";
      renderMaintenancePanel(repo);
      if (!repo) {
        repoDetail.hidden = true;
        repoDetailActions.hidden = true;
        repoDetailEmpty.hidden = false;
        return;
      }

      repoDetail.hidden = false;
      repoDetailActions.hidden = false;
      repoDetailEmpty.hidden = true;

      const detailRows = [
        createDetailRow("Repository", repo.name + " (" + repo.repository_id + ")"),
        createDetailRow("Feed URL", repo.feedHref || "n/a"),
        createDetailRow("Updated", repo.updated_at || "n/a"),
        createDetailRow("Created", repo.created_at || "n/a"),
        createDetailRow("Config JSON", JSON.stringify(repo.config || {}, null, 2)),
      ];
      for (const row of detailRows) {
        repoDetail.appendChild(row);
      }

      const editButton = document.createElement("button");
      editButton.type = "button";
      editButton.textContent = "Edit In Form";
      editButton.addEventListener("click", () => {
        loadRepositoryIntoForm(repo);
      });
      repoDetailActions.appendChild(editButton);

      const openLink = document.createElement("a");
      openLink.href = repo.feedHref;
      openLink.target = "_blank";
      openLink.rel = "noopener noreferrer";
      openLink.textContent = "Open feed";
      repoDetailActions.appendChild(openLink);
    }

    function renderMaintenancePanel(repo) {
      if (!repo) {
        maintenanceEmpty.hidden = false;
        maintenanceSelected.hidden = true;
        maintenanceSelectedName.textContent = "";
        maintenanceRebuildFeeds.disabled = true;
        maintenanceReindexSubjects.disabled = true;
        maintenanceReindexAuthorities.disabled = true;
        maintenanceInvalidateRepoCache.disabled = true;
        maintenanceClearData.disabled = true;
        maintenanceDeleteRepository.disabled = true;
        return;
      }
      maintenanceEmpty.hidden = true;
      maintenanceSelected.hidden = false;
      maintenanceSelectedName.textContent = repo.name + " (" + repo.repository_id + ")";
      maintenanceRebuildFeeds.disabled = false;
      maintenanceReindexSubjects.disabled = false;
      maintenanceReindexAuthorities.disabled = false;
      maintenanceInvalidateRepoCache.disabled = false;
      const isDefaultRepo = repo.repository_id === DEFAULT_REPOSITORY_ID;
      maintenanceClearData.disabled = isDefaultRepo;
      maintenanceDeleteRepository.disabled = isDefaultRepo;
    }

    async function invalidateCache(repositoryId) {
      const body = {};
      if (repositoryId) {
        body.repository_id = repositoryId;
      }
      const response = await fetch("/admin/cache/invalidate", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(response);
      showActionResponse(repositoryId ? "Invalidate Repo Cache" : "Invalidate All Cache", response, data);
      if (repositoryId) {
        setHarvestResult(response.ok, response.ok ? "Repo Cache Invalidated" : "Repo Cache Invalidate Failed");
      } else {
        setHarvestResult(response.ok, response.ok ? "Global Cache Invalidated" : "Global Cache Invalidate Failed");
      }
    }

    function renderRepositories(payload, preferredRepositoryId) {
      const repositories = payload.repositories || [];
      const selectedRepositoryId = preferredRepositoryId || repoSelect.value;
      repoList.innerHTML = "";
      repoSelect.innerHTML = "";
      let detailRepository = null;
      const defaultRepo = repositories.find((repo) => repo.isDefaultRepository);
      if (defaultRepo) {
        repoSummary.textContent = "Default feed: " + defaultRepo.name + " (" + defaultRepo.repository_id + ").";
      } else {
        repoSummary.textContent = "No default repository is currently configured.";
      }
      for (const repo of repositories) {
        if (!(repo.repository_id in subjectBackfillCursors)) {
          subjectBackfillCursors[repo.repository_id] = "";
        }
        if (selectedRepository && selectedRepository.repository_id === repo.repository_id) {
          detailRepository = repo;
        }
        const option = document.createElement("option");
        option.value = repo.repository_id;
        option.textContent = repo.name + " (" + repo.repository_id + ")";
        repoSelect.appendChild(option);

        const card = document.createElement("div");
        card.className = "repo";
        const metaBits = [
          "source_type: " + repo.source_type,
          "source: " + (repo.sourceDomain || "n/a"),
          "items: " + (repo.publicationCount != null ? repo.publicationCount : 0),
          "checkpoints: " + (repo.checkpointCount != null ? repo.checkpointCount : 0)
        ];
        const sourceUrl = repo && repo.config && typeof repo.config.url === "string" ? repo.config.url : "";
        const nameNode = document.createElement("strong");
        nameNode.textContent = repo.name;
        const idNode = document.createElement("code");
        idNode.textContent = repo.repository_id;
        const metaNode = document.createElement("small");
        metaNode.textContent = metaBits.join(" | ");
        card.appendChild(nameNode);
        card.appendChild(idNode);
        card.appendChild(metaNode);

        if (sourceUrl) {
          const sourceNode = document.createElement("div");
          sourceNode.className = "source-url";
          const sourceLabel = document.createElement("strong");
          sourceLabel.textContent = "url: ";
          sourceNode.appendChild(sourceLabel);
          sourceNode.appendChild(document.createTextNode(sourceUrl));
          card.appendChild(sourceNode);
        }

        const pillsNode = document.createElement("div");
        pillsNode.className = "pill-row";
        if (repo.isDefaultRepository) {
          const defaultPill = document.createElement("span");
          defaultPill.className = "pill default";
          defaultPill.textContent = "Default Feed";
          pillsNode.appendChild(defaultPill);
        }
        if (repo.is_active) {
          const activePill = document.createElement("span");
          activePill.className = "pill active";
          activePill.textContent = "Active";
          pillsNode.appendChild(activePill);
        }
        card.appendChild(pillsNode);

        const actions = document.createElement("div");
        actions.className = "repo-actions";

        const openLink = document.createElement("a");
        openLink.href = repo.feedHref;
        openLink.target = "_blank";
        openLink.rel = "noopener noreferrer";
        openLink.textContent = "Open feed";
        openLink.addEventListener("click", (event) => event.stopPropagation());
        actions.appendChild(openLink);

        card.appendChild(actions);
        card.addEventListener("click", () => selectRepository(repo));
        repoList.appendChild(card);
      }
      if (selectedRepositoryId && repositories.some((repo) => repo.repository_id === selectedRepositoryId)) {
        repoSelect.value = selectedRepositoryId;
      }
      if (!detailRepository && selectedRepositoryId) {
        detailRepository = repositories.find((repo) => repo.repository_id === selectedRepositoryId) || null;
      }
      renderRepositoryDetail(detailRepository);
      updateHarvestActionState();
    }

    async function backfillSubjects(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, repoSelect.value);
      if (!repositoryId) {
        show({ error: "Select a repository first." });
        return;
      }
      const batchInput = window.prompt("Reindex batch size (1-5000):", "500");
      if (batchInput === null) {
        return;
      }
      const batchSize = Number(batchInput.trim() || "500");
      if (!Number.isFinite(batchSize) || batchSize < 1 || batchSize > 5000) {
        show({ error: "Batch size must be between 1 and 5000." });
        return;
      }
      const body = { batch_size: Math.floor(batchSize) };
      const cursor = subjectBackfillCursors[repositoryId];
      if (cursor) {
        body.start_after = cursor;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/backfill/subjects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(response);
      showActionResponse("Reindex Subjects", response, data);
      if (response.ok) {
        updateSubjectBackfillCursor(repositoryId, data);
        await refreshRepositoriesSilently(repositoryId);
      }
    }

    async function backfillSubjectAuthorities(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, repoSelect.value);
      if (!repositoryId) {
        show({ error: "Select a repository first." });
        return;
      }
      const batchInput = window.prompt("Subject authority reindex batch size (1-5000):", "500");
      if (batchInput === null) {
        return;
      }
      const batchSize = Number(batchInput.trim() || "500");
      if (!Number.isFinite(batchSize) || batchSize < 1 || batchSize > 5000) {
        show({ error: "Batch size must be between 1 and 5000." });
        return;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/backfill/subject-authorities", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ batch_size: Math.floor(batchSize) }),
      });
      const data = await readJson(response);
      showActionResponse("Reindex Subject Authorities", response, data);
      if (response.ok) {
        await refreshRepositoriesSilently(repositoryId);
      }
    }

    async function backfillSubjectsByOffset(repositoryId, batchSize) {
      let offset = 0;
      let processed = 0;
      while (true) {
        const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/backfill/subjects", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ batch_size: Math.floor(batchSize), offset: offset }),
        });
        const data = await readJson(response);
        showActionResponse("Rebuild Feed Derivations · Subjects", response, data);
        if (!response.ok) {
          throw new Error("Subject reindex failed.");
        }
        processed += Number(data && data.processed_publications ? data.processed_publications : 0);
        if (!data || data.has_more === false) {
          return processed;
        }
        offset += Math.floor(batchSize);
      }
    }

    async function backfillSubjectAuthoritiesByOffset(repositoryId, batchSize) {
      let offset = 0;
      let processed = 0;
      while (true) {
        const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/backfill/subject-authorities", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ batch_size: Math.floor(batchSize), offset: offset }),
        });
        const data = await readJson(response);
        showActionResponse("Rebuild Feed Derivations · Subject Authorities", response, data);
        if (!response.ok) {
          throw new Error("Subject authority reindex failed.");
        }
        processed += Number(data && data.processed_publications ? data.processed_publications : 0);
        if (!data || data.has_more === false) {
          return processed;
        }
        offset += Math.floor(batchSize);
      }
    }

    async function rebuildFeedDerivations(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, repoSelect.value);
      if (!repositoryId) {
        show({ error: "Select a repository first." });
        return;
      }
      const batchInput = window.prompt("Rebuild batch size per request (1-5000):", "500");
      if (batchInput === null) {
        return;
      }
      const batchSize = Number(batchInput.trim() || "500");
      if (!Number.isFinite(batchSize) || batchSize < 1 || batchSize > 5000) {
        show({ error: "Batch size must be between 1 and 5000." });
        return;
      }
      const confirmed = window.confirm(
        "Rebuild all feed derivations for '" + repositoryId + "'?\n\nThis runs:\n1) Reindex Subjects\n2) Reindex Subject Authorities\n3) Invalidate Repo Cache"
      );
      if (!confirmed) {
        return;
      }
      maintenanceRebuildFeeds.disabled = true;
      try {
        const subjectsProcessed = await backfillSubjectsByOffset(repositoryId, batchSize);
        const authoritiesProcessed = await backfillSubjectAuthoritiesByOffset(repositoryId, batchSize);
        await invalidateCache(repositoryId);
        subjectBackfillCursors[repositoryId] = "";
        await refreshRepositoriesSilently(repositoryId);
        show({
          action: "Rebuild Feed Derivations",
          repository_id: repositoryId,
          subjects_processed: subjectsProcessed,
          subject_authorities_processed: authoritiesProcessed,
          cache_invalidated: true,
        });
        setHarvestResult(true, "Feed Derivations Rebuilt");
      } finally {
        if (selectedRepository && selectedRepository.repository_id === repositoryId) {
          maintenanceRebuildFeeds.disabled = false;
        }
      }
    }

    async function clearRepositoryData(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, repoIdInput.value);
      if (!repositoryId) {
        show({ error: "Select or enter a repository first." });
        return;
      }
      if (repositoryId === DEFAULT_REPOSITORY_ID) {
        show({ error: "The default repository cannot be cleared." });
        return;
      }
      const confirmed = window.confirm("Clear harvested data and checkpoints for '" + repositoryId + "' while keeping its configuration?");
      if (!confirmed) {
        return;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/clear-data", {
        method: "POST",
      });
      const data = await readJson(response);
      showActionResponse("Clear Repository Data", response, data);
      if (response.ok) {
        subjectBackfillCursors[repositoryId] = "";
        await refreshRepositoriesSilently(repositoryId);
      }
    }

    function loadRepositoryIntoForm(repo) {
      repoIdInput.value = repo.repository_id;
      repoNameInput.value = repo.name;
      repoTypeInput.value = repo.source_type;
      repoConfigInput.value = JSON.stringify(repo.config || {}, null, 2);
      repoActiveInput.checked = Boolean(repo.is_active);
      updateRepoFormButtonState();
    }

    function selectRepository(repo) {
      selectedRepository = repo;
      repoSelect.value = repo.repository_id;
      renderRepositoryDetail(repo);
    }

    async function loadRepositories(preferredRepositoryId, options) {
      const settings = options || {};
      const response = await fetch("/repositories");
      const data = await readJson(response);
      if (!response.ok) {
        show(data);
        return;
      }
      renderRepositories(data, preferredRepositoryId);
      if (!settings.silent) {
        show(data);
      }
    }

    async function saveRepository(event) {
      event.preventDefault();
      const repositoryId = repoIdInput.value.trim();
      const body = {
        source_type: repoTypeInput.value,
        name: repoNameInput.value.trim(),
        config: normalizeConfigText(repoConfigInput.value),
        is_active: repoActiveInput.checked,
      };
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(response);
      showActionResponse("Save Repository", response, data);
      if (response.ok) {
        await refreshRepositoriesSilently(repositoryId);
        repoSelect.value = repositoryId;
      }
    }

    async function deleteRepository(explicitRepositoryId) {
      const repositoryId = resolveRepositoryId(explicitRepositoryId, repoIdInput.value);
      if (!repositoryId) {
        show({ error: "Select or enter a repository first." });
        return;
      }
      if (repositoryId === DEFAULT_REPOSITORY_ID) {
        show({ error: "The default repository cannot be deleted." });
        return;
      }
      const confirmed = window.confirm("Delete repository '" + repositoryId + "' and all of its harvested data?");
      if (!confirmed) {
        return;
      }
      const response = await fetch("/repositories/" + encodeURIComponent(repositoryId), {
        method: "DELETE",
      });
      const data = await readJson(response);
      showActionResponse("Delete Repository", response, data);
      if (response.ok) {
        delete subjectBackfillCursors[repositoryId];
        if (selectedRepository && selectedRepository.repository_id === repositoryId) {
          renderRepositoryDetail(null);
        }
        repoForm.reset();
        repoConfigInput.value = "{}";
        repoActiveInput.checked = true;
        updateRepoFormButtonState();
        await refreshRepositoriesSilently();
      }
    }

    async function runHarvest(event) {
      event.preventDefault();
      setHarvestBusy(true, "Harvesting");
      try {
        const repositoryId = repoSelect.value;
        const body = {
          url: harvestUrlInput.value.trim(),
          follow_next: harvestFollowNextInput.checked,
          incremental: harvestIncrementalInput.checked,
          timeout_seconds: Number(harvestTimeoutInput.value) || 120,
        };
        const maxPages = harvestMaxPagesInput.value.trim();
        const maxRecords = harvestMaxRecordsInput.value.trim();
        if (maxPages) body.max_pages = Number(maxPages);
        if (maxRecords) body.max_records = Number(maxRecords);

        const response = await fetch("/repositories/" + encodeURIComponent(repositoryId) + "/ingest/opds-json", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
        const data = await readJson(response);
        showActionResponse("Start Harvest", response, data);
        setHarvestResult(response.ok, response.ok ? "Harvest Succeeded" : "Harvest Failed");
      } catch (error) {
        setHarvestResult(false, "Harvest Failed");
        throw error;
      } finally {
        setHarvestBusy(false);
      }
    }

    async function fetchDirectories() {
      const url = harvestUrlInput.value.trim();
      if (!url) {
        show({ error: "Enter a remote feed URL first." });
        return;
      }
      setHarvestBusy(true, "Fetching");
      try {
        const timeoutSeconds = Number(harvestTimeoutInput.value) || 120;
        const response = await fetch("/ingest/opds-json/directories", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: url, timeout_seconds: timeoutSeconds }),
        });
        const data = await readJson(response);
        showActionResponse("Fetch Directories", response, data);
        setHarvestResult(response.ok, response.ok ? "Directories Loaded" : "Directory Fetch Failed");
        if (response.ok) {
          renderDirectoryEntries(data.directories || []);
        }
      } catch (error) {
        setHarvestResult(false, "Directory Fetch Failed");
        throw error;
      } finally {
        setHarvestBusy(false);
      }
    }

    async function importSelectedDirectories(modeOverride) {
      const selectedEntries = getCheckedDirectoryEntries();
      if (!selectedEntries.length) {
        show({ error: "Select at least one directory to import." });
        return;
      }
      setHarvestBusy(true, "Importing");
      try {
        const mode = modeOverride || currentDirectoryImportMode();
        const payload = {
          root_url: harvestUrlInput.value.trim(),
          directories: selectedEntries.map((entry) => ({
            title: entry.title || entry.href,
            href: entry.href,
          })),
          mode: mode,
          follow_next: harvestFollowNextInput.checked,
          incremental: harvestIncrementalInput.checked,
          timeout_seconds: Number(harvestTimeoutInput.value) || 120,
        };
        const maxPages = harvestMaxPagesInput.value.trim();
        const maxRecords = harvestMaxRecordsInput.value.trim();
        if (maxPages) payload.max_pages = Number(maxPages);
        if (maxRecords) payload.max_records = Number(maxRecords);
        if (mode === "single-repository-collections") {
          payload.target_repository_id = repoSelect.value;
        }

        const response = await fetch("/ingest/opds-json/directories/import", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await readJson(response);
        showActionResponse("Import Directories", response, data);
        setHarvestResult(response.ok, response.ok ? "Import Succeeded" : "Import Failed");
        if (response.ok) {
          await refreshRepositoriesSilently(repoSelect.value);
          updateDirectoryImportSummary();
          updateHarvestActionState();
        }
      } catch (error) {
        setHarvestResult(false, "Import Failed");
        throw error;
      } finally {
        setHarvestBusy(false);
      }
    }

    async function loadCheckpoints() {
      const repositoryId = repoSelect.value;
      try {
        const response = await fetch("/harvest/checkpoints?repository_id=" + encodeURIComponent(repositoryId));
        const data = await readJson(response);
        showActionResponse("Load Checkpoints", response, data);
        setHarvestResult(response.ok, response.ok ? "Checkpoints Loaded" : "Checkpoint Load Failed");
      } catch (error) {
        setHarvestResult(false, "Checkpoint Load Failed");
        throw error;
      }
    }

    repoForm.addEventListener("submit", (event) => {
      saveRepository(event).catch((error) => show({ error: String(error) }));
    });
    for (const button of viewButtons) {
      button.addEventListener("click", () => {
        const viewName = button.getAttribute("data-view-button");
        if (viewName) {
          setActiveView(viewName);
        }
      });
    }
    repoIdInput.addEventListener("input", () => {
      updateRepoFormButtonState();
    });
    repoNameInput.addEventListener("input", () => {
      updateRepoFormButtonState();
    });
    repoConfigInput.addEventListener("input", () => {
      updateRepoFormButtonState();
    });
    harvestForm.addEventListener("submit", (event) => {
      runHarvest(event).catch((error) => show({ error: String(error) }));
    });
    refreshReposButton.addEventListener("click", () => {
      loadRepositories().catch((error) => show({ error: String(error) }));
    });
    refreshReposManageButton.addEventListener("click", () => {
      loadRepositories().catch((error) => show({ error: String(error) }));
    });
    maintenanceReindexSubjects.addEventListener("click", () => {
      if (!selectedRepository) {
        show({ error: "Select a repository first." });
        return;
      }
      backfillSubjects(selectedRepository.repository_id).catch((error) => show({ error: String(error) }));
    });
    maintenanceRebuildFeeds.addEventListener("click", () => {
      if (!selectedRepository) {
        show({ error: "Select a repository first." });
        return;
      }
      rebuildFeedDerivations(selectedRepository.repository_id).catch((error) => show({ error: String(error) }));
    });
    maintenanceReindexAuthorities.addEventListener("click", () => {
      if (!selectedRepository) {
        show({ error: "Select a repository first." });
        return;
      }
      backfillSubjectAuthorities(selectedRepository.repository_id).catch((error) => show({ error: String(error) }));
    });
    maintenanceInvalidateRepoCache.addEventListener("click", () => {
      if (!selectedRepository) {
        show({ error: "Select a repository first." });
        return;
      }
      invalidateCache(selectedRepository.repository_id).catch((error) => show({ error: String(error) }));
    });
    maintenanceInvalidateAllCache.addEventListener("click", () => {
      invalidateCache("").catch((error) => show({ error: String(error) }));
    });
    maintenanceClearData.addEventListener("click", () => {
      if (!selectedRepository) {
        show({ error: "Select a repository first." });
        return;
      }
      clearRepositoryData(selectedRepository.repository_id).catch((error) => show({ error: String(error) }));
    });
    maintenanceDeleteRepository.addEventListener("click", () => {
      if (!selectedRepository) {
        show({ error: "Select a repository first." });
        return;
      }
      deleteRepository(selectedRepository.repository_id).catch((error) => show({ error: String(error) }));
    });
    harvestUrlInput.addEventListener("input", () => {
      updateHarvestActionState();
    });
    document.getElementById("load-checkpoints").addEventListener("click", () => {
      loadCheckpoints().catch((error) => show({ error: String(error) }));
    });
    document.getElementById("fetch-directories").addEventListener("click", () => {
      fetchDirectories().catch((error) => show({ error: String(error) }));
    });
    document.getElementById("import-as-repositories").addEventListener("click", () => {
      importSelectedDirectories("split-repositories").catch((error) => show({ error: String(error) }));
    });
    document.getElementById("import-into-repository").addEventListener("click", () => {
      importSelectedDirectories("single-repository-collections").catch((error) => show({ error: String(error) }));
    });
    directoryModeSplitInput.addEventListener("change", () => {
      syncDirectoryImportModeUi();
    });
    directoryModeSingleInput.addEventListener("change", () => {
      syncDirectoryImportModeUi();
    });
    repoSelect.addEventListener("change", () => {
      updateDirectoryImportSummary();
      updateHarvestActionState();
    });
    directorySelectAll.addEventListener("change", () => {
      const checked = directorySelectAll.checked;
      for (const input of directoryList.querySelectorAll("input[data-directory-index]")) {
        input.checked = checked;
      }
      maybeAutofillSplitRepositoryConfig();
      updateDirectoryImportSummary();
      updateHarvestActionState();
    });
    loadRepositories().catch((error) => show({ error: String(error) }));
    setActiveView(activeView);
    renderDirectoryEntries([]);
    syncDirectoryImportModeUi();
    updateHarvestActionState();
    updateRepoFormButtonState();
