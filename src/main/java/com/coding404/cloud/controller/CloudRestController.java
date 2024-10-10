package com.coding404.cloud.controller;


import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.sync.RequestBody;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class CloudRestController {

    //버킷목록
    @GetMapping("/get_bucket_list")
    public String get_bucket_list() {

        //버킷 목록을 가져오는 코드
        //자격증명
        S3Client s3 = S3Client.builder()
                .region(Region.AP_NORTHEAST_2)
                .credentialsProvider(ProfileCredentialsProvider.create()) //유저폴더/.aws/credentails파일을 읽음
                .build();

        ListBucketsResponse response = s3.listBuckets();
        List<Bucket> bucketList = response.buckets();
        for (Bucket bucket: bucketList) {
            System.out.println("Bucket name "+bucket.name());
        }
        return "success";
    }


    //객체목록
    @GetMapping("/list_bucket_objects")
    public String list_bucket_objects() {

        //자격증명
        S3Client s3 = S3Client.builder()
                .region(Region.AP_NORTHEAST_2)
                .credentialsProvider(ProfileCredentialsProvider.create()) //유저폴더/.aws/credentails파일을 읽음
                .build();

        String bucketName = "demo-yewon31"; //버킷명

        try {
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .maxKeys(1) //페이지네이션...
                    .build();

            ListObjectsV2Iterable listRes = s3.listObjectsV2Paginator(listReq);
            listRes.stream()
                    .flatMap(r -> r.contents().stream())
                    .forEach(content -> System.out.println(" Key: " + content.key() + " size = " + content.size()));

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        return "success";
    }

    //객체 업로드
    @PostMapping("/cloudUpload")
    public String cloudUpload(@RequestParam("file_data") MultipartFile file) {

        String filename = file.getOriginalFilename();
        System.out.println( "한글: " + filename);

        //자격증명
        S3Client s3 = S3Client.builder()
                .region(Region.AP_NORTHEAST_2)
                .credentialsProvider(ProfileCredentialsProvider.create()) //유저폴더/.aws/credentails파일을 읽음
                .build();

        String bucketName = "demo-yewon31"; //버킷명

        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("author", "Mary Doe");
            metadata.put("version", "1.0.0.0");

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName) //버킷명
                    .key(filename) //올릴파일명
                    .metadata(metadata) //메타데이터
                    .contentType(file.getContentType() ) //데이터에 대한 컨텐츠타입
                    .build();

            s3.putObject(putOb, RequestBody.fromBytes( file.getBytes() ) );
            System.out.println("Successfully placed " + filename + " into bucket " + bucketName);

        } catch (Exception e) {
            System.err.println(e.getMessage());
            //System.exit(1);
        }
        return "success";
    }


    //객체 삭제하기
    @DeleteMapping("/delete_bucket_objects")
    public String delete_bucket_objects(@RequestParam("bucket_obj_name") String bucket_obj_name) {

        //자격증명
        S3Client s3 = S3Client.builder()
                .region(Region.AP_NORTHEAST_2)
                .credentialsProvider(ProfileCredentialsProvider.create()) //유저폴더/.aws/credentails파일을 읽음
                .build();

        ArrayList<ObjectIdentifier> keys = new ArrayList<>();
        PutObjectRequest putOb;
        ObjectIdentifier objectId;

        String keyName = bucket_obj_name; //파일명
        String bucketName = "demo-yewon31"; //버킷명

        //키생성후에 리스트에 담는코드
        objectId = ObjectIdentifier.builder()
                .key(keyName)
                .build();

        putOb = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(keyName)
                .build();

        s3.putObject(putOb, RequestBody.fromString(keyName));
        keys.add(objectId);

        System.out.println(keys.size() + " objects successfully created.");

        // Delete multiple objects in one request.
        Delete del = Delete.builder()
                .objects(keys)
                .build();

        try {
            DeleteObjectsRequest multiObjectDeleteRequest = DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(del)
                    .build();

            DeleteObjectsResponse response = s3.deleteObjects(multiObjectDeleteRequest);
            //....response객체에서 실행결과에 대한 처리가 가능함.
            System.out.println("Multiple objects are deleted!");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "success";
    }


    //람다 호출시키기
    @GetMapping("/lambda_call")
    public String lambda_call() {

        //람다를 핸들링 하기위한 람다를 생성합니다.
        LambdaClient awsLambda = LambdaClient.builder()
                .region(Region.AP_NORTHEAST_2)//서울 리전
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();

        //실행시킬 람다 함수명
        String functionName = "demo-lambda-example";

        InvokeResponse res = null ;
        try {
            //Need a SdkBytes instance for the payload
            String json = "{\"Hello \":\"자바에서 보낸 매개변수\"}";
            SdkBytes payload = SdkBytes.fromUtf8String(json) ;

            //Setup an InvokeRequest
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName) //실행시킬 람다함수의 이름이 들어갑니다.
                    .payload(payload)
                    .build();

            res = awsLambda.invoke(request); //호출
            String value = res.payload().asUtf8String() ;

            //value값이 한글이라면 JSONObject객체 or GSon을 사용해서 복원해야 합니다.
            System.out.println(value); //

        } catch(LambdaException e) {
            System.err.println(e.getMessage());
        }

        return "success";
    }



}
