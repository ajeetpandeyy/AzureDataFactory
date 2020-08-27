function json_to_table(pipe_detail){
    arr = pipe_detail.activities
    table_body = ""
    var arrayLength = arr.length;
    for (var i = 0; i < arrayLength; i++) {
        element = arr[i];
        new_row_string = `<tr>
        <td>${element.activity_name}</td>
        <td>${element.status}</td>`;
        if ('databricks_url' in element){
        new_row_string += `<td><a href="${element.databricks_url}">Job Link</a></td>`
        }
        new_row_string += "</tr>"
        table_body = table_body + new_row_string;
    }

    table_output = `<table><tr><th>Activity Name</th><th>Status</th><th>Databricks URL</th></tr>${table_body}</table>`;
    return table_output;
}

function disable_link(row_div){
    link = row_div.getElementsByClassName("detail_link")[0];
    link.href = "javascript:void(0)";
}

function query_activities(elementid, run_id){
    row_div = document.getElementById(elementid);

    request_url = `/api/pipeline/${run_id}/`;
    console.log(request_url);

    fetch(request_url)
    .then(res => res.json())//response type
    .then(data => json_to_table(data)) //log the data;
    .then(data => row_div.insertAdjacentHTML('beforeend', data)); //log the data;

    disable_link(row_div);

}