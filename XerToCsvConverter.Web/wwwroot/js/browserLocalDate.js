(function () {
    "use strict";

    window.xerToCsv = window.xerToCsv || {};
    window.xerToCsv.formatBrowserLocalIsoDate = function (date) {
        const year = String(date.getFullYear()).padStart(4, "0");
        const month = String(date.getMonth() + 1).padStart(2, "0");
        const day = String(date.getDate()).padStart(2, "0");
        return `${year}-${month}-${day}`;
    };
    window.xerToCsv.getBrowserLocalIsoDate = function () {
        return window.xerToCsv.formatBrowserLocalIsoDate(new Date());
    };
}());
