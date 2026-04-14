(function () {
  function toNumber(value) {
    const parsed = Number(String(value || "").replace(",", "."));
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
              label: "Comissao prevista",
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
              label: "Comissao por ano",
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
              label: "Comissao por curso",
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
    const installmentsInput = document.getElementById("installments-count");
    const totalValueInput = document.getElementById("total-value");
    const commissionInput = document.getElementById("commission-percent");
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

    function updatePreview() {
      const total = toNumber(totalValueInput.value);
      const commissionPercent = toNumber(commissionInput.value);
      const installments = Math.max(1, parseInt(installmentsInput.value || "1", 10));
      const installmentValue = total / installments;
      const commissionPerInstallment = (installmentValue * commissionPercent) / 100;
      preview.textContent = formatCurrency(commissionPerInstallment);
    }

    companySelect.addEventListener("change", filterCoursesByCompany);
    courseSelect.addEventListener("change", applyCourseDefaultCommission);
    paymentFormat.addEventListener("change", updateInstallmentsBehavior);
    totalValueInput.addEventListener("input", updatePreview);
    installmentsInput.addEventListener("input", updatePreview);
    commissionInput.addEventListener("input", updatePreview);

    filterCoursesByCompany();
    updateInstallmentsBehavior();
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

  renderCharts();
  bindSaleForm();
  bindForgotPassword();
})();
