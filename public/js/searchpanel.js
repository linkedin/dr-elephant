
$('.search_opt').on("click",function(){
  $('#search_concept').text($(this).text())
  console.log($(this).text());
});

$('#search_button').on("click", function() {
  let text = $('#primary_search').val();
  let type = $('#search_concept').text();
  let encodedurl = encodeURIComponent(text);
  if(type=='Workflow') {
    let transition = "/new#/workflow?workflowid=" + encodedurl;
    window.location = transition
  } else if (type=='Job') {
    let transition = "/new#/job?jobid=" + encodedurl;
    window.location = transition;
  } else if (type=='Application') {
    let transition = "/new#/app?applicationid=" + encodedurl;
    window.location = transition;
  }
});