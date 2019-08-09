
function fillDetails(){
	$.ajax({
				url: "http://127.0.0.1:8000/details?format=json",
				type: 'GET',
				dataType: "json",
				success: function(data, status, xhr){
					document.getElementById("lives-impacted").innerHTML = data["lives_impacted"];
					document.getElementById("total-qXR").innerHTML = data["qxr_scans"];
					document.getElementById("total-qER").innerHTML = data["qer_scans"];
					document.getElementById("runtime-qXR").innerHTML = data["qxr_runtime"] + "s";
					document.getElementById("runtime-qER").innerHTML = data["qer_runtime"] + "s";
					document.getElementById("avg-scans").innerHTML = data["avg_scans"];
					document.getElementById("tb-positive").innerHTML = data["tb_positive"];
					document.getElementById("num-site").innerHTML = data["num_site"];
				},
				error: function(jqXHR, exception){
					console.log(exception);
				}

	});
}


function fillChart(){
	$.ajax({
				url: "http://127.0.0.1:8000/chart_points",
				type: 'GET',
				dataType: "json",
				success: function(data, status, xhr){
					var uplim = parseInt(data['max']);
					var qxr = data['qxr_points'];
					for(var entry in qxr){
						qxr[entry].t = new Date(qxr[entry].t);
						console.log(typeof qxr[entry].t);
						qxr[entry].y = parseInt(qxr[entry].y);
					}
					var qer = data['qer_points'];
					for(var entry in qer){
						qer[entry].t = new Date(qer[entry].t);
						qer[entry].y = parseInt(qer[entry].y);
					}
					var lab = [];
					for(var i = 0; i < qxr.length; i++){
						lab.push(qxr[i].t)
					}
					var ctx = document.getElementById("client-graph").getContext("2d");
					var myChart = new Chart(ctx, {
						type: 'line',
						data:{
							labels: lab,
							datasets: [{
								label: 'qXR',
								borderColor: 'rgba(52, 152, 219, 1)',
								backgroundColor: 'rgba(52, 152, 219, 0.3)',
								fill: false,
								data: qxr,

							},
							{
								label: 'qER',
								borderColor: 'rgba(245, 176, 65, 1)',
								backgroundColor: 'rgba(245, 176, 65, 0.3)',
								fill: false,
								data: qer,

							}]
						},
						options: {
							scales: {
								xAxes: [{
									type: 'time',
									time: {
										unit: 'month',
										unitStepSize: 1,
										displayFormats:{
											month: 'MMM YYYY'
										}
									},
									ticks:{
										callback: function(dataLabel, index){
											return index % 4 === 0 ? dataLabel : ''; 
										}
									},
									scaleLabel: {
										display: true,
										labelString: 'Date'
									}
								}],
								yAxes: [{
									scaleLabel:{
										display: true,
										labelString: 'Number of clients'
									},
									ticks: {
										suggestedMax: uplim
									}
								}]
							}
						}
					});
				},
				error: function(jqXHR, exception){
					console.log(exception);
				}

	});
}



function fillCurDeplTable(){
	$(document).ready(function () {
    	var table_one = $('#curr_depl_table').DataTable({
        	"columns": [
                { "data": "full_name" },
                { "data": "location" },
                { "data": "qXR"},
                { "data": "qER" },
                { "data": "total" },
                { "data": "avg_scans" },
                { "data": "last_date" }
            ],
            "columnDefs": [
            	{
            		"targets": "no-sort", 
            		"orderable": false,
            		"render": function(data, type, row){
            			return data == 'true' ? "<p class='text-center'><i class='fa fa-check' aria-hidden='true'></i></p>" : ''
            		}
            	},
            	{
            		"targets": "source_name",
            		"width": "30%"
            	}
            ]
        });
        $.ajax({
            url: 'http://127.0.0.1:8000/current_deployments_table',
            dataType: 'json',
            success: function (json) {
            	table_one.rows.add(json.data).draw();
            }
        });
    });
}



function fillOnpremiseTable(){
		$(document).ready(function () {
    	var table_two = $('#onpremise_table').DataTable({
        	"columns": [
                { "data": "full_name" },
                { "data": "location" },
                { "data": "qXR"},
                { "data": "qER" },
                { "data": "qCheck" },
                { "data": "total_uploads" }
            ],
            "columnDefs": [
            	{
            		"targets": "no-sort", 
            		"orderable": false,
            		"render": function(data, type, row){
            			return data == 'true' ? "<p class='text-center'><i class='fa fa-check' aria-hidden='true'></i></p>" : ''
            		}
            	},
            	{
            		"targets": "source_name",
            		"width": "30%"
            	}
            ]
        });
        $.ajax({
            url: 'http://127.0.0.1:8000/onpremise_table',
            dataType: 'json',
            success: function (json) {
            	table_two.rows.add(json.data).draw();
            }
        });
    });
}

function initMap() {
	$.ajax({
				url: "http://127.0.0.1:8000/details?format=json",
				type: 'GET',
				dataType: "json",
				success: function(data, status, xhr){
					var locations = data['locations'];
					var center = {lat: 23.5880, lng: 58.3829};
					var bounds = new google.maps.LatLngBounds();
					var map = new google.maps.Map(document.getElementById('map'), {
						zoom: 4,
					    center: center,
					    mapTypeId: google.maps.MapTypeId.HYBRID
					});
					var infowindow =  new google.maps.InfoWindow({});
					var marker, count;
					for (count = 0; count < locations.length; count++) {
					    marker = new google.maps.Marker({
					    	position: new google.maps.LatLng(locations[count][1], locations[count][2]),
					    	map: map,
					    	title: locations[count][0]
					    });
					    google.maps.event.addListener(marker, 'click', (function (marker, count) {
					    	return function () {
					    		infowindow.setContent(locations[count][0]);
					        	infowindow.open(map, marker);
					      }
					    })(marker, count));
					    loc = new google.maps.LatLng(marker.position.lat(), marker.position.lng());
					    bounds.extend(loc);
					  }
					map.fitBounds(bounds);
					map.panToBounds(bounds);
				},
				error: function(jqXHR, exception){
					console.log(exception);
				}
	});
}


(function(global){
	document.addEventListener("DOMContentLoaded", function(event){
		fillDetails();
		fillChart();
		fillCurDeplTable();
		fillOnpremiseTable();
		initMap();

		//details and current deployment table updates every 1 hour
		setInterval(function(){
			fillDetails();
			fillCurDeplTable();
		}, 3600000);
  	});

})(window);