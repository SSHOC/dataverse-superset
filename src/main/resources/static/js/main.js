function toggleEmbedCode(embedChartId, embedChartButtonId) {
	var embedChartStyle = document.getElementById(embedChartId).style;
	embedChartStyle.display = (embedChartStyle.display === "none" ? "block" : "none");
	document.getElementById(embedChartButtonId).textContent = (embedChart.style.display === "none" ? "Embed chart" : "Hide embed code");
}
function changeChart(chartFrameId, chartSelectorId) {
	document.getElementById('chartIFrame').setAttribute('src', document.getElementById('chartSelector').value);
	document.getElementById('embedChart').textContent =
		'<iframe width="600" height="400" seamless frameBorder="0" scrolling="no" src="' + document.getElementById('chartSelector').value + '"></iframe>';
}
