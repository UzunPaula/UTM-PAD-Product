const api = {
    status: "/api/status",
    orders: "/api/orders",
    retry: "/api/orders/retry"
};

const elements = {
    form: document.querySelector("#orderForm"),
    orderId: document.querySelector("#orderId"),
    product: document.querySelector("#product"),
    description: document.querySelector("#description"),
    submitButton: document.querySelector("#submitButton"),
    refreshButton: document.querySelector("#refreshButton"),
    result: document.querySelector("#submissionResult"),
    brokerState: document.querySelector("#brokerState"),
    consumerState: document.querySelector("#consumerState"),
    topicLabel: document.querySelector("#topicLabel"),
    systemBadge: document.querySelector("#systemBadge"),
    systemBadgeText: document.querySelector("#systemBadgeText"),
    pendingCount: document.querySelector("#pendingCount"),
    inFlightCount: document.querySelector("#inFlightCount"),
    acknowledgedCount: document.querySelector("#acknowledgedCount"),
    deadLetterCount: document.querySelector("#deadLetterCount"),
    deadLetterListCount: document.querySelector("#deadLetterListCount"),
    deadLetterBody: document.querySelector("#deadLetterBody"),
    historyBody: document.querySelector("#historyBody"),
    historyCount: document.querySelector("#historyCount")
};

const history = [];
let statusRefreshInProgress = false;

function createOrderId() {
    const suffix = crypto.randomUUID().slice(0, 8).toUpperCase();
    return `ORD-${suffix}`;
}

function setFieldError(fieldName, message) {
    const input = elements[fieldName];
    const error = document.querySelector(`[data-error-for="${fieldName}"]`);
    input.classList.toggle("invalid", Boolean(message));
    input.setAttribute("aria-invalid", String(Boolean(message)));
    error.textContent = message;
}

function validateOrder(order) {
    const errors = {
        orderId: order.orderId ? "" : "ID-ul comenzii este obligatoriu.",
        product: order.product ? "" : "Produsul este obligatoriu.",
        description: order.description ? "" : "Descrierea este obligatorie."
    };

    Object.entries(errors).forEach(([field, message]) => setFieldError(field, message));
    return !Object.values(errors).some(Boolean);
}

function setComponentState(element, isOnline) {
    const state = isOnline === true
        ? { label: "Online", className: "online" }
        : isOnline === false
            ? { label: "Offline", className: "offline" }
            : { label: "Necunoscut", className: "unknown" };
    element.textContent = state.label;
    element.className = `state-pill ${state.className}`;
}

function renderSystemState(brokerState, consumerState) {
    setComponentState(elements.brokerState, brokerState);
    setComponentState(elements.consumerState, consumerState);

    const bothOnline = brokerState === true && consumerState === true;
    const brokerOnly = brokerState === true;
    elements.systemBadge.className = `system-badge ${bothOnline ? "online" : brokerOnly ? "degraded" : "offline"}`;
    elements.systemBadgeText.textContent = bothOnline
        ? "Sistem operațional"
        : brokerOnly ? "Broker disponibil" : "Sistem offline";
}

function renderDeadLetters(deadLetters) {
    const entries = Array.isArray(deadLetters) ? deadLetters : [];
    elements.deadLetterListCount.textContent =
        `${entries.length} ${entries.length === 1 ? "mesaj" : "mesaje"}`;
    elements.deadLetterBody.innerHTML = "";

    if (entries.length === 0) {
        const row = document.createElement("tr");
        row.className = "empty-row";
        row.innerHTML = '<td colspan="5">Nu există mesaje în dead-letter queue.</td>';
        elements.deadLetterBody.append(row);
        return;
    }

    entries.forEach(entry => {
        const row = document.createElement("tr");
        const values = [
            entry.messageId,
            entry.topic,
            entry.retryCount,
            entry.reason,
            entry.deadLetteredAtUtc
                ? new Date(entry.deadLetteredAtUtc).toLocaleString("ro-RO")
                : "Necunoscut"
        ];

        values.forEach((value, index) => {
            const cell = document.createElement("td");
            cell.textContent = value ?? "—";
            if (index === 0) {
                cell.className = "message-id";
                cell.title = value ?? "";
            } else if (index === 3) {
                cell.className = "reason-cell";
            }
            row.append(cell);
        });
        elements.deadLetterBody.append(row);
    });
}

function updateHistoryDeliveryStates(status) {
    const acknowledged = new Set(
        (status.recentAcknowledgements || [])
            .map(entry => String(entry.messageId).toLowerCase()));
    const deadLetters = new Set(
        (status.deadLetters || [])
            .map(entry => String(entry.messageId).toLowerCase()));

    history.forEach(item => {
        if (!item.messageId) return;
        const messageId = String(item.messageId).toLowerCase();
        if (deadLetters.has(messageId)) {
            item.state = "dead-letter";
        } else if (acknowledged.has(messageId)) {
            item.state = "delivered";
        } else if (item.state === "dead-letter") {
            item.state = "pending";
        }
    });
    renderHistory();
}

async function refreshStatus() {
    if (statusRefreshInProgress) return;
    statusRefreshInProgress = true;
    elements.refreshButton.disabled = true;
    try {
        const response = await fetch(api.status, { headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const status = await response.json();
        renderSystemState(status.brokerConnected, status.consumerConnected);
        elements.topicLabel.textContent = status.producerId || "Producer";
        elements.consumerState.title = Array.isArray(status.consumerEndpoints) ? status.consumerEndpoints.join("\n") : "";
        if (status.consumerConnected && status.consumerCount > 0) elements.consumerState.textContent = `Online (${status.consumerCount})`;
        elements.pendingCount.textContent = Number.isInteger(status.pendingMessages) ? status.pendingMessages : "—";
        elements.inFlightCount.textContent = Number.isInteger(status.inFlightMessages) ? status.inFlightMessages : "—";
        elements.acknowledgedCount.textContent = Number.isInteger(status.acknowledgedMessages) ? status.acknowledgedMessages : "—";
        elements.deadLetterCount.textContent = Number.isInteger(status.deadLetterMessages) ? status.deadLetterMessages : "—";
        renderDeadLetters(status.deadLetters);
        updateHistoryDeliveryStates(status);
    } catch {
        renderSystemState(false, false);
        elements.pendingCount.textContent = "—";
        elements.inFlightCount.textContent = "—";
        elements.acknowledgedCount.textContent = "—";
        elements.deadLetterCount.textContent = "—";
        renderDeadLetters([]);
    } finally {
        statusRefreshInProgress = false;
        elements.refreshButton.disabled = false;
    }
}

function showResult(success, title, detail) {
    elements.result.hidden = false;
    elements.result.className = `result-card ${success ? "success" : "error"}`;
    elements.result.innerHTML = "";

    const heading = document.createElement("strong");
    const description = document.createElement("span");
    heading.textContent = title;
    description.textContent = detail;
    elements.result.append(heading, description);
}

function renderHistory() {
    elements.historyCount.textContent = `${history.length} ${history.length === 1 ? "comandă" : "comenzi"}`;
    elements.historyBody.innerHTML = "";

    if (history.length === 0) {
        const row = document.createElement("tr");
        row.className = "empty-row";
        row.innerHTML = '<td colspan="6">Nu au fost trimise comenzi în această sesiune.</td>';
        elements.historyBody.append(row);
        return;
    }

    history.forEach(item => {
        const row = document.createElement("tr");
        const values = [item.orderId, item.product, item.description];
        values.forEach(value => {
            const cell = document.createElement("td");
            cell.textContent = value;
            row.append(cell);
        });

        const states = {
            published: { label: "Publicat", className: "pending" },
            delivered: { label: "Livrat", className: "success" },
            "dead-letter": { label: "Dead-letter", className: "error" },
            unknown: { label: "Necunoscut", className: "pending" },
            failed: { label: "Respins", className: "error" }
        };
        const state = states[item.state] || states.failed;
        const stateCell = document.createElement("td");
        stateCell.className = `table-state ${state.className}`;
        stateCell.textContent = state.label;
        row.append(stateCell);

        const messageCell = document.createElement("td");
        messageCell.className = "message-id";
        messageCell.title = item.messageId || item.detail;
        messageCell.textContent = item.messageId || item.detail;
        row.append(messageCell);

        const actionCell = document.createElement("td");
        if (item.messageId && item.correlationId) {
            const retryButton = document.createElement("button");
            retryButton.type = "button";
            retryButton.className = "table-action";
            retryButton.textContent = "Retrimite mesajul";
            retryButton.addEventListener("click", () => retryOrder(item, retryButton));
            actionCell.append(retryButton);
        }
        row.append(actionCell);
        elements.historyBody.append(row);
    });
}

async function retryOrder(item, button) {
    button.disabled = true;
    try {
        const response = await fetch(api.retry, {
            method: "POST",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: JSON.stringify({
                order: {
                    orderId: item.orderId,
                    product: item.product,
                    description: item.description
                },
                messageId: item.messageId,
                correlationId: item.correlationId
            })
        });
        const result = await response.json().catch(() => ({}));
        if (result.outcome === "Duplicate") {
            showResult(false, "Duplicare mesaj", `messageId: ${item.messageId}`);
            return;
        }
        if (!response.ok) throw new Error(result.reason || `Răspuns HTTP ${response.status}.`);
        showResult(true, "Mesajul a fost retrimis.", `messageId: ${item.messageId}`);
    } catch (error) {
        const detail = error instanceof Error ? error.message : "Backendul nu este disponibil.";
        showResult(false, "Retrimiterea nu a putut fi confirmată.", detail);
    } finally {
        button.disabled = false;
        await refreshStatus();
    }
}

async function submitOrder(event) {
    event.preventDefault();
    const order = {
        orderId: elements.orderId.value.trim(),
        product: elements.product.value.trim(),
        description: elements.description.value.trim()
    };

    if (!validateOrder(order)) return;

    elements.submitButton.disabled = true;
    try {
        const response = await fetch(api.orders, {
            method: "POST",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: JSON.stringify(order)
        });
        const result = await response.json().catch(() => ({}));
        if (result.outcome === "Unknown") {
            const detail = `${result.reason || "Nu se poate confirma rezultatul."} messageId: ${result.messageId || "necomunicat"}`;
            showResult(false, "Starea comenzii este necunoscută.", detail);
            history.unshift({ ...order, state: "unknown", messageId: result.messageId, detail });
            return;
        }
        if (!response.ok || result.accepted === false) {
            throw new Error(result.reason || `Cererea a fost respinsă (HTTP ${response.status}).`);
        }

        const detail = `messageId: ${result.messageId || "necomunicat"} · correlationId: ${result.correlationId || "necomunicat"}`;
        showResult(true, "Comanda a fost acceptată de Broker.", detail);
        history.unshift({
            ...order,
            state: "published",
            messageId: result.messageId || "Acceptat",
            correlationId: result.correlationId
        });
        elements.form.reset();
        elements.orderId.value = createOrderId();
        await refreshStatus();
    } catch (error) {
        const detail = error instanceof Error ? error.message : "Backendul nu este disponibil.";
        showResult(false, "Comanda nu a fost trimisă.", detail);
        history.unshift({ ...order, state: "failed", detail });
    } finally {
        elements.submitButton.disabled = false;
        renderHistory();
    }
}

document.querySelector("#generateOrderId").addEventListener("click", () => {
    elements.orderId.value = createOrderId();
    setFieldError("orderId", "");
});
elements.refreshButton.addEventListener("click", refreshStatus);
elements.form.addEventListener("submit", submitOrder);
elements.orderId.value = createOrderId();
renderHistory();
refreshStatus();
window.setInterval(refreshStatus, 500);
