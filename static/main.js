(function () {
  function toNumber(value) {
    const raw = String(value || "").trim().replace(/\s+/g, "");
    if (!raw) {
      return 0;
    }
    let normalized = raw;
    if (raw.includes(",") && raw.includes(".")) {
      const lastComma = raw.lastIndexOf(",");
      const lastDot = raw.lastIndexOf(".");
      if (lastComma > lastDot) {
        normalized = raw.replace(/\./g, "").replace(",", ".");
      } else {
        normalized = raw.replace(/,/g, "");
      }
    } else if (raw.includes(",")) {
      const idx = raw.lastIndexOf(",");
      const left = raw.slice(0, idx);
      const right = raw.slice(idx + 1);
      normalized = right.length <= 2 ? `${left.replace(/,/g, "")}.${right}` : raw.replace(/,/g, "");
    } else if (raw.includes(".")) {
      const parts = raw.split(".");
      if (parts.length === 2) {
        const [left, right] = parts;
        if (right.length <= 2) {
          normalized = `${left}.${right}`;
        } else if (right.length === 3) {
          normalized = `${left}${right}`;
        } else if (/^0+$/.test(right) && right.length > 2) {
          normalized = `${left}${right.slice(0, -2)}`;
        } else {
          normalized = `${left}${right}`;
        }
      } else {
        const last = parts.at(-1) || "";
        if (last.length <= 2) {
          normalized = `${parts.slice(0, -1).join("")}.${last}`;
        } else {
          normalized = parts.join("");
        }
      }
    }

    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function formatCurrency(value) {
    return new Intl.NumberFormat("pt-BR", {
      style: "currency",
      currency: "BRL",
    }).format(value || 0);
  }

  function renderCharts() {
    const holder = document.getElementById("chart-data");
    if (!holder || typeof Chart === "undefined") {
      return;
    }

    let payload;
    try {
      payload = JSON.parse(holder.textContent);
    } catch {
      return;
    }

    const monthlyCtx = document.getElementById("chart-monthly");
    const yearlyCtx = document.getElementById("chart-yearly");
    const courseCtx = document.getElementById("chart-course");

    if (monthlyCtx) {
      new Chart(monthlyCtx, {
        type: "bar",
        data: {
          labels: payload.monthly.labels,
          datasets: [
            {
              label: "Valor previsto",
              data: payload.monthly.value,
              backgroundColor: "rgba(13, 122, 95, 0.75)",
              borderRadius: 8,
            },
            {
              label: "Comissão prevista",
              data: payload.monthly.commission,
              backgroundColor: "rgba(255, 122, 26, 0.8)",
              borderRadius: 8,
            },
          ],
        },
        options: { responsive: true, maintainAspectRatio: false },
      });
    }

    if (yearlyCtx) {
      new Chart(yearlyCtx, {
        type: "line",
        data: {
          labels: payload.yearly.labels,
          datasets: [
            {
              label: "Comissão por ano",
              data: payload.yearly.commission,
              borderColor: "rgba(13, 122, 95, 0.9)",
              backgroundColor: "rgba(13, 122, 95, 0.2)",
              fill: true,
              tension: 0.3,
            },
          ],
        },
        options: { responsive: true, maintainAspectRatio: false },
      });
    }

    if (courseCtx) {
      new Chart(courseCtx, {
        type: "doughnut",
        data: {
          labels: payload.course.labels,
          datasets: [
            {
              label: "Comissão por curso",
              data: payload.course.commission,
              backgroundColor: [
                "#0d7a5f",
                "#ff7a1a",
                "#2b8bba",
                "#f4b400",
                "#4f46e5",
                "#0f766e",
                "#be185d",
              ],
            },
          ],
        },
        options: { responsive: true, maintainAspectRatio: false },
      });
    }
  }

  function bindSaleForm() {
    const form = document.getElementById("sale-form");
    if (!form) {
      return;
    }

    const companySelect = document.getElementById("company-select");
    const courseSelect = document.getElementById("course-select");
    const paymentFormat = document.getElementById("payment-format");
    const commissionModeWrap = document.getElementById("commission-payment-mode-wrap");
    const commissionMode = document.getElementById("commission-payment-mode");
    const installmentsInput = document.getElementById("installments-count");
    const totalValueInput = document.getElementById("total-value");
    const commissionInput = document.getElementById("commission-percent");
    const previewLabel = document.getElementById("commission-preview-label");
    const preview = document.getElementById("commission-preview");

    function filterCoursesByCompany() {
      const selectedCompany = companySelect.value;
      const currentCourse = courseSelect.value;
      let hasCurrent = false;

      Array.from(courseSelect.options).forEach((option, idx) => {
        if (idx === 0) {
          option.hidden = false;
          return;
        }
        const companyId = option.dataset.companyId;
        const show = !selectedCompany || selectedCompany === companyId;
        option.hidden = !show;
        if (show && option.value === currentCourse) {
          hasCurrent = true;
        }
      });

      if (currentCourse && !hasCurrent) {
        courseSelect.value = "";
      }
    }

    function applyCourseDefaultCommission() {
      const selected = courseSelect.options[courseSelect.selectedIndex];
      if (!selected) {
        return;
      }
      const defaultPercent = selected.dataset.defaultCommission;
      if (defaultPercent) {
        commissionInput.value = defaultPercent;
      }
      updatePreview();
    }

    function updateInstallmentsBehavior() {
      if (paymentFormat.value === "avista") {
        installmentsInput.value = 1;
        installmentsInput.setAttribute("readonly", "readonly");
      } else {
        installmentsInput.removeAttribute("readonly");
        if (toNumber(installmentsInput.value) < 2) {
          installmentsInput.value = 2;
        }
      }
      updatePreview();
    }

    function updateCommissionModeBehavior() {
      const isRecurring = paymentFormat.value === "recorrencia";
      if (commissionModeWrap) {
        commissionModeWrap.style.display = isRecurring ? "" : "none";
      }
      if (commissionMode) {
        commissionMode.disabled = !isRecurring;
        if (!isRecurring) {
          commissionMode.value = "per_installment";
        }
      }
    }

    function updatePreview() {
      const total = toNumber(totalValueInput.value);
      const commissionPercent = toNumber(commissionInput.value);
      const installments = Math.max(1, parseInt(installmentsInput.value || "1", 10));
      const installmentValue = total / installments;
      const totalCommission = (total * commissionPercent) / 100;
      const commissionPerInstallment = (installmentValue * commissionPercent) / 100;

      let previewValue = commissionPerInstallment;
      if (previewLabel) {
        previewLabel.textContent = "Comissão prevista por parcela:";
      }
      if (paymentFormat.value === "avista") {
        previewValue = totalCommission;
        if (previewLabel) {
          previewLabel.textContent = "Comissão prevista da venda:";
        }
      } else if (
        paymentFormat.value === "recorrencia" &&
        commissionMode &&
        commissionMode.value === "upfront_first_installment"
      ) {
        previewValue = totalCommission;
        if (previewLabel) {
          previewLabel.textContent = "Comissão total na 1ª parcela:";
        }
      }
      preview.textContent = formatCurrency(previewValue);
    }

    companySelect.addEventListener("change", filterCoursesByCompany);
    courseSelect.addEventListener("change", applyCourseDefaultCommission);
    paymentFormat.addEventListener("change", () => {
      updateInstallmentsBehavior();
      updateCommissionModeBehavior();
    });
    if (commissionMode) {
      commissionMode.addEventListener("change", updatePreview);
    }
    totalValueInput.addEventListener("input", updatePreview);
    installmentsInput.addEventListener("input", updatePreview);
    commissionInput.addEventListener("input", updatePreview);
    form.addEventListener("submit", (event) => {
      if (paymentFormat.value !== "recorrencia") {
        return;
      }
      const selectedLabel =
        commissionMode && commissionMode.options[commissionMode.selectedIndex]
          ? commissionMode.options[commissionMode.selectedIndex].textContent
          : "Comissão em recorrência (por parcela)";
      const confirmed = window.confirm(
        `Confirma que a comissão desta venda recorrente será: ${selectedLabel}?`
      );
      if (!confirmed) {
        event.preventDefault();
      }
    });

    filterCoursesByCompany();
    updateInstallmentsBehavior();
    updateCommissionModeBehavior();
    updatePreview();
  }

  function bindForgotPassword() {
    const button = document.getElementById("copy-password-btn");
    const input = document.getElementById("generated-password");
    if (!button || !input) {
      return;
    }

    (async () => {
      try {
        await navigator.clipboard.writeText(input.value);
        button.textContent = "Copiada automaticamente";
      } catch {
        // Some browsers block clipboard write without gesture.
      }
    })();

    button.addEventListener("click", async () => {
      input.select();
      try {
        await navigator.clipboard.writeText(input.value);
        button.textContent = "Copiado";
      } catch {
        document.execCommand("copy");
        button.textContent = "Copiado";
      }
    });
  }

  function bindReminderCopy() {
    const buttons = document.querySelectorAll(".copy-reminder-btn");
    if (!buttons.length) {
      return;
    }

    buttons.forEach((button) => {
      button.addEventListener("click", async () => {
        const message = button.dataset.message || "";
        if (!message) {
          return;
        }
        try {
          await navigator.clipboard.writeText(message);
          button.textContent = "Mensagem copiada";
        } catch {
          const helper = document.createElement("textarea");
          helper.value = message;
          document.body.appendChild(helper);
          helper.select();
          document.execCommand("copy");
          helper.remove();
          button.textContent = "Mensagem copiada";
        }
      });
    });
  }

  function bindInternationalPhone() {
    const input = document.getElementById("customer-phone");
    if (!input) {
      return;
    }
    input.placeholder = "+12 345 678 9101";

    if (typeof window.intlTelInput !== "function") {
      return;
    }

    const iti = window.intlTelInput(input, {
      initialCountry: "auto",
      autoPlaceholder: "polite",
      nationalMode: false,
      formatOnDisplay: true,
      preferredCountries: ["br", "us", "pt", "es", "gb", "fr", "it", "de"],
      geoIpLookup: (callback) => {
        fetch("https://ipapi.co/json/")
          .then((resp) => resp.json())
          .then((data) => callback((data && data.country_code ? data.country_code : "br").toLowerCase()))
          .catch(() => callback("br"));
      },
    });

    const form = document.getElementById("sale-form");
    if (form) {
      form.addEventListener("submit", () => {
        const full = iti.getNumber();
        if (full) {
          input.value = full;
        }
      });
    }
  }

  renderCharts();
  bindSaleForm();
  bindForgotPassword();
  bindReminderCopy();
  bindInternationalPhone();
})();
