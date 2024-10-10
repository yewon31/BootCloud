const dropbox = document.querySelector('.file_box');
const input_filename = document.querySelector('.file_name');

const file_btn = document.querySelector('.upload_btn');

const list_bucket_objects = document.querySelector('.list_bucket_objects');

const bucket_obj_name = document.querySelector('input[name="bucket_obj_name"]');
const delete_bucket_objects = document.querySelector(".delete_bucket_objects");

let file_data;

//박스 안에 drag 하고 있을 때
dropbox.addEventListener('dragover', function (e) {
  e.preventDefault();
  this.style.backgroundColor = 'rgb(13 110 253 / 25%)';
});

//박스 밖으로 drag가 나갈 때
dropbox.addEventListener('dragleave', function (e) {
  this.style.backgroundColor = 'white';
});

//박스 안에 drop 했을 때
dropbox.addEventListener('drop', function (e) {
  e.preventDefault();
  //데이터 크기 검사  
  let byteSize= e.dataTransfer.files[0].size;
  let maxSize = 50;
    
  if( byteSize / 1000000 > maxSize) {
	  alert("파일은 최대 50MB이하만 허용됩니다");
	  return;
  } else {
	  //백그라운드 색상변경
  	  this.style.backgroundColor = 'white';
	  //파일 이름을 text로 표시
	  let filename = e.dataTransfer.files[0].name;
	  input_filename.innerHTML = filename;
		
	  //파일 데이터를 변수에 저장
	  file_data = e.dataTransfer.files[0];
  }
    
});

//파일 업로드 버튼 클릭시 데이터 전송
file_btn.addEventListener('click', function (e) {
	
	let formData = new FormData();
	formData.append('file_data' , file_data);
	
	fetch('/cloudUpload', {method: 'post', body: formData})
	.then(response => response.text() )
	.then(data => {
		alert(data);
	})
	.catch(err => alert('업로드에 실패했습니다:' + err) );
		
	
});

//버킷 객체의 전체목록 조회
list_bucket_objects.addEventListener('click', function (e) {
	
	fetch('/list_bucket_objects')
	.then(response => response.text() )
	.then(data => {
		alert(data);
	})
	
});

//버킷 객체의 목록 삭제하기
delete_bucket_objects.addEventListener('click', function (e) {
	
	//console.log('인풋태그값:' + bucket_obj_name.value)
	
	let formData = new FormData();
	formData.append('bucket_obj_name' , bucket_obj_name.value);
	
	fetch('/delete_bucket_objects', {method: 'delete', 
									 body: formData })
	.then(response => response.text() )
	.then(data => {
		alert(data);
	})
	
});


///////////////////////////////////////////////////////////////
//람다 함수 호출
const lambda_call = document.querySelector(".lambda_call")

const lambda_call_step1 = document.querySelector(".lambda_call_step1");
const lambda_call_step1_sel = document.querySelector(".lambda_call_step1_sel");

const lambda_call_step2 = document.querySelector(".lambda_call_step2");
const lambda_call_step2_sel = document.querySelector(".lambda_call_step2_sel");

//람다콜 테스트
lambda_call.addEventListener('click', function (e) {
	
	fetch('/lambda_call')
	.then(response => response.text() )
	.then(data => {
		alert(data);
	});
});

//데이터 전처리
lambda_call_step1.addEventListener('click', function(e) {
	
	fetch('/lambda_call_step1')
	.then(response => response.json() )
	.then(data => {
		
		//결과 출력
		console.log('데이터 전처리가 완료되었습니다. 파일명:' + data.body);
		alert('데이터 전처리가 완료되었습니다. 파일명:' + data.body);
		
		//결과 출력
		lambda_call_step1_sel.innerHTML = data.body;
	})

		
});


//데이터 시각화처리(select태그의 파일이름을 spring으로 전송합니다)
lambda_call_step2.addEventListener('click', function(e) {
	
	
	//테스트 데이터
	var value = '20211130주택도시보증공사_전국_민간아파트_분양가격동향_전처리1.csv';
	//var value = lambda_call_step1_sel.innerHTML;
	
	fetch('/lambda_call_step2?file_name=' + value)
	.then(response => response.json() )
	.then(data => {

		console.log('시각화가 완료되었습니다. 경로:', data.url);
		lambda_call_step2_sel.src = data.url;
		//결과를 확인했으면 마지막으로 s3에서는 이미지에 접근 권한을 열어주세요.
	})

		
});














