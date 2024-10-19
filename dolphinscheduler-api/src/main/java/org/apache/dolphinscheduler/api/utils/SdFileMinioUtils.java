package org.apache.dolphinscheduler.api.utils;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.file.FileNameUtil;
import cn.hutool.core.util.StrUtil;
import io.minio.*;
import io.minio.Result;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.io.*;
import java.net.URLDecoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 单例模式 工具类型、
 */
@Component
@Slf4j
@RefreshScope
@SuppressWarnings("all")
public class SdFileMinioUtils {

    /**
     * Description  获取 MinioClient 对象
     * Create By Mr.Fang
     *
     * @return com.ming.utils.MinioUtils
     * @time 2022/7/10 10:18
     **/
    public MinioClient minioClient(String host,String appkey,String appSecret) {
        return MinioClient.builder().endpoint(host).credentials(appkey, appSecret).build();
    }

    /**
     * @param fileName
     * @return
     * @author xhw
     * @生成文件名
     */
    public String getUUIDFileName(String fileName) {
        int idx = fileName.lastIndexOf(".");
        String extention = fileName.substring(idx);
        String newFileName = DateUtil.format(new Date(), "yyyyMMddHHmmssSSS") + "_" + UUID.randomUUID().toString().replace("-", "") + extention;
        return newFileName;
    }

    @SneakyThrows
    public void removeBucket(String bucketName, boolean bucketNotNull,String host,String  key,String appSecret) {
        if (bucketNotNull) {
            deleteBucketAllObject(bucketName,host,key,appSecret);
        }
        minioClient(host,key,appSecret).removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
    }
    /**
     * 删除桶中所有的对象
     *
     * @param bucketName 桶对象
     */
    @SneakyThrows
    public void deleteBucketAllObject(String bucketName,String host,String  key,String appSecret) {
        List<String> list = listObjectNames(bucketName,host,key,appSecret);
        if (!list.isEmpty()) {
            deleteObject(bucketName, list,host,key,appSecret);
        }
    }

    /**
     * Description 批量删除文件对象
     * Create By Mr.Fang
     *
     * @param objects 文件名称或完整文件路径
     * @return java.util.List<io.minio.messages.DeleteError>
     * @time 2022/7/10 9:54
     **/
    public List<DeleteError> deleteObject(String bucketName, List<String> objects,String host,String  key,String appSecret) {
        List<DeleteError> deleteErrors = new ArrayList<>();
        List<DeleteObject> deleteObjects = objects.stream().map(value -> new DeleteObject(value)).collect(Collectors.toList());
        Iterable<Result<DeleteError>> results =
                minioClient(host,key,appSecret).removeObjects(
                        RemoveObjectsArgs
                                .builder()
                                .bucket(bucketName)
                                .objects(deleteObjects)
                                .build());
        try {
            for (Result<DeleteError> result : results) {
                DeleteError error = result.get();
                deleteErrors.add(error);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deleteErrors;
    }

    /**
     * 查询桶中所有的对象名
     *
     * @param bucketName 桶名
     * @return objectNames
     */
    @SneakyThrows
    public List<String> listObjectNames(String bucketName,String host,String  key,String appSecret) {
        List<String> objectNameList = new ArrayList<>();
        if (existBucket(bucketName,host,key,appSecret)) {
            Iterable<Result<Item>> results = listObjects(bucketName, true,host,key,appSecret);
            for (Result<Item> result : results) {
                String objectName = result.get().objectName();
                objectNameList.add(objectName);
            }
        }
        return objectNameList;
    }


    /**
     * 文件合并，将分块文件组成一个新的文件
     *
     * @param bucketName       合并文件生成文件所在的桶
     * @param objectName       原始文件名
     * @param sourceObjectList 分块文件集合
     * @return OssFile
     */
    @SneakyThrows
    public OssFile composeObject(List<ComposeSource> sourceObjectList, String bucketName, String objectName,String host,String  key,String appSecret) {
        minioClient(host,key,appSecret).composeObject(ComposeObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .sources(sourceObjectList)
                .build());
        //String presignedObjectUrl = getPresignedObjectUrl(bucketName, objectName);
        String objectUrl = getObjectUrl(bucketName, objectName, host,key,appSecret);
        OssFile ossFile = new OssFile();
        ossFile.setUrl(objectUrl);
        ossFile.setBucketName(bucketName);
        return ossFile;
    }

    /**
     * 文件合并，将分块文件组成一个新的文件
     *
     * @param originBucketName 分块文件所在的桶
     * @param targetBucketName 合并文件生成文件所在的桶
     * @param objectName       存储于桶中的对象名
     * @return OssFile
     */
    @SneakyThrows
    public OssFile composeObject(String originBucketName, String targetBucketName, String objectName,String host,String  key,String appSecret) {
        Iterable<io.minio.Result<Item>> results = listObjects(originBucketName, true,host,key,appSecret);
        List<String> objectNameList = new ArrayList<>();
        for (Result<Item> result : results) {
            Item item = result.get();
            objectNameList.add(item.objectName());
        }
        if (ObjectUtils.isEmpty(objectNameList)) {
            throw new IllegalArgumentException(originBucketName + "桶中没有文件，请检查");
        }
        List<ComposeSource> composeSourceList = new ArrayList<>(objectNameList.size());
        // 对文件名集合进行升序排序
        objectNameList.sort(Comparator.comparing(name -> {
            String prefix = FileNameUtil.getPrefix(name);
            return Integer.parseInt(prefix.substring(prefix.indexOf("part_") + "part_".length()));
        }));
        for (String object : objectNameList) {
            composeSourceList.add(ComposeSource.builder()
                    .bucket(originBucketName)
                    .object(object)
                    .build());
        }
        return composeObject(composeSourceList, targetBucketName, objectName,host,key,appSecret);
    }

    /**
     * 查询桶的对象信息
     *
     * @param bucketName 桶名
     * @param recursive  是否递归查询
     * @return 桶的对象信息
     */
    @SneakyThrows
    public Iterable<Result<Item>> listObjects(String bucketName, boolean recursive,String host,String  key,String appSecret) {
        return minioClient(host,key,appSecret).listObjects(
                ListObjectsArgs.builder().bucket(bucketName).recursive(recursive).build());
    }
    /**
     * 获取规则生成的文件名
     *
     * @param originalFilename
     * @return
     */
    public String generateFileInMinioName(String originalFilename) {
        return "files" + StrUtil.SLASH + DateUtil.format(new Date(), "yyyy-MM-dd") + StrUtil.SLASH + UUID.randomUUID() + StrUtil.UNDERLINE + originalFilename;
    }

    /**
     * Description 判断桶是否存在
     * Create By Mr.Fang
     *
     * @param bucketName
     * @return java.lang.Boolean
     * @time 2022/7/10 9:57
     **/
    public Boolean existBucket(String bucketName,String host,String  key,String appSecret) {
        try {
            return minioClient(host,key,appSecret).bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Description 创建一个桶
     * Create By Mr.Fang
     *
     * @param bucketName 桶名称
     * @return java.lang.Boolean
     * @time 2022/7/10 9:56
     **/
    public Boolean createBucket(String bucketName,String host,String  key,String appSecret) {
        try {
            minioClient(host,key,appSecret).makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Description 上传本地文件
     * Create By Mr.Fang
     *
     * @param bucketName 桶的名称
     * @param path       本地文件路径
     * @param object     文件名
     * @return java.lang.Boolean
     * @time 2022/7/10 9:55
     **/
    public Boolean uploadObject(String bucketName, String path, String object,String host,String  key,String appSecret) {
        try {
            minioClient(host,key,appSecret).uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(object)
                            .filename(path)
                            .build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * Description 以流的方式上传文件
     * Create By Mr.Fang
     *
     * @param bucketName       桶的名称
     * @param in               input 流
     * @param originalFileName 文件名,minioClient中object的参数
     * @param isRename         是否重命名
     * @return java.lang.Boolean
     * @time 2022/7/10 9:55
     **/
    public Boolean uploadObject(String bucketName, InputStream in, String originalFileName, boolean isRename,String host,String  key,String appSecret) {
        if (isRename) {
            String uuidFileName = generateFileInMinioName(originalFileName);
            originalFileName = uuidFileName;
        }
        try {
            minioClient(host,key,appSecret).putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(originalFileName)
                            .stream(in, in.available(), -1) // 第二个参数是文件的大小
                            .build()
            );
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 大文件分片上传(本地)  如需流的方式大文件分片,可以参考本方法,参数传入inputStream
     *
     * @return
     */
    public OssFile putChunkObject(FileCustomUploadVO fileCustomUploadVO) {
        OssFile ossFile = new OssFile();
        //File file = new File("D:\\测试文件\\L4\\多文件解析\\20240321091150.record.00000");
        File file = new File(fileCustomUploadVO.getLocalFilePath());
        if (!file.exists()) {
            throw new ServiceException(500, "文件不存在");
        }
        // 创建一个临时桶用来存储分片数据
        String tempBucketName = "temp-chunk" + UUID.randomUUID().toString().replace("-", "");
        Boolean bucket = this.createBucket(tempBucketName,fileCustomUploadVO.getHost(),fileCustomUploadVO.getKey(),fileCustomUploadVO.getAppSecret());
        if (!bucket) {
            throw new ServiceException(500, "创建临时桶失败");
        }
        try (FileInputStream inputStream = new FileInputStream(file);) {
            long fileSize = file.length();
            // 每个分片64M(minio默认最小分块大小为5M,如果小于5M,合并分片文件时会报错)
            long chunkSize = 64 * 1024 * 1024;
            // 计算分片数量
            int numParts = (int) (fileSize / chunkSize) + ((fileSize % chunkSize) > 0 ? 1 : 0);
            long offset = 0;
            byte[] chunk = new byte[(int) chunkSize];
            int chunkIndex = 1;
            int bytesRead;
            while ((bytesRead = inputStream.read(chunk)) != -1) {
                minioClient(fileCustomUploadVO.getHost(),fileCustomUploadVO.getKey(),fileCustomUploadVO.getAppSecret()).putObject(
                        PutObjectArgs.builder()
                                .bucket(tempBucketName)
                                .object(FileNameUtil.getPrefix(file) + "_part_" + chunkIndex + "." + FileNameUtil.getSuffix(file))
                                .stream(new ByteArrayInputStream(chunk, 0, bytesRead), bytesRead, -1)
                                .build()
                );
                offset += bytesRead;
                chunkIndex++;
            }
            if (offset < fileSize) {
                throw new IOException("Unexpected EOF while reading file data.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ErrorResponseException e) {
            throw new RuntimeException(e);
        } catch (InsufficientDataException e) {
            throw new RuntimeException(e);
        } catch (InternalException e) {
            throw new RuntimeException(e);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        } catch (InvalidResponseException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        } catch (ServerException e) {
            throw new RuntimeException(e);
        } catch (XmlParserException e) {
            throw new RuntimeException(e);
        }
        ossFile.setOriginalFileName(file.getName());
        ossFile.setBucketName(tempBucketName);
        return ossFile;
    }

    /**
     * Description 获取所有桶
     * Create By Mr.Fang
     *
     * @return java.util.List<io.minio.messages.Bucket>
     * @time 2022/7/10 9:50
     **/
    public List<Bucket> listBuckets(String host,String  key,String appSecret) {
        try {
            return minioClient(host,key,appSecret).listBuckets();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }



    /**
     * Description 生成预览链接，最大7天有效期；
     * 如果想永久有效，在 minio 控制台设置仓库访问规则总几率
     * Create by Mr.Fang
     *
     * @param bucketName  桶的名称
     * @param object      文件名称
     * @param contentType 预览类型 image/gif", "image/jpeg", "image/jpg", "image/png", "application/pdf
     * @return java.lang.String
     * @Time 9:43 2022/7/10
     * @Params
     **/
    public String getPreviewUrl(String bucketName, String object, String contentType,String host,String  key,String appSecret) {
        // map参数根据实际情况,如果未知不传,可以注释掉
        Map<String, String> reqParams = new HashMap<>();
        reqParams.put("response-content-type", contentType != null ? contentType : "application/pdf");
        String url = null;
        try {
            url = minioClient(host,key,appSecret).getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucketName)
                            .object(object)
                            .expiry(7, TimeUnit.DAYS)
                            .extraQueryParams(reqParams)
                            .build());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return url;
    }

    /**
     * 获取url
     * 该方法neverExpires传true表示获取永久访问路径, 但是前提需要设置minio服务桶为public，并且将Prefix前缀填*,Access填readonly只读，代表该桶所有文件可读。
     *
     * @param bucketName
     * @param objectName
     * @param expireSeconds 过期时长(单位秒,默认7天)
     * @param neverExpires
     * @return
     */
    @SneakyThrows
    public String getObjectUrl(String bucketName, String objectName, String host,String  key,String appSecret) {
        Boolean flag = existBucket(bucketName,host,key,appSecret);
        Integer  expireSeconds = 604800;
        Boolean neverExpires = true;
        String url = "";
        if (flag) {
            // 获取URL
            url = minioClient(host,key,appSecret).getPresignedObjectUrl(
                    GetPresignedObjectUrlArgs.builder()
                            .method(Method.GET)
                            .bucket(bucketName)
                            .object(objectName)
                            .expiry(expireSeconds)
                            .build());
        }
        // TODO neverExpires根据需求,后期minio桶设置访问权限可能需要删除该参数,全部使用临时签名
        if (StringUtils.isNotBlank(url) && neverExpires) {
            // 生成永不过期的URL,(前提需要设置桶为public，并且将Prefix前缀填*,Access填readonly只读，代表该桶所有文件可读。)
            // 设置后直接通过ip+端口号+桶名+文件名 获取文件，从而解决7天有效期的问题。
            url = URLDecoder.decode(url.split("\\?")[0], "UTF-8");
        } else {
            url = URLDecoder.decode(url, "UTF-8");
        }
        return url;
    }



    /**
     * Description 文件转字节数组
     * Create By Mr.Fang
     *
     * @param path 文件路径
     * @return byte[] 字节数组
     * @time 2022/7/10 10:55
     **/
    public byte[] fileToBytes(String path) {
        FileInputStream fis = null;
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            fis = new FileInputStream(path);
            int temp;
            byte[] bt = new byte[1024 * 10];
            while ((temp = fis.read(bt)) != -1) {
                bos.write(bt, 0, temp);
            }
            bos.flush();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (Objects.nonNull(fis)) {
                    fis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bos.toByteArray();
    }



}



