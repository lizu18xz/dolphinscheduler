package org.apache.dolphinscheduler.plugin.task.k8s;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.parameters.DataSetK8sTaskParameters;

public class Test {

    public static void main(String[] args) {
        String aa="{\n" +
                "\t\"localParams\": [],\n" +
                "\t\"resourceList\": [],\n" +
                "\t\"namespace\": \"{\\\"name\\\":\\\"projecttmp\\\",\\\"cluster\\\":\\\"yiqi\\\"}\",\n" +
                "\t\"minCpuCores\": 1,\n" +
                "\t\"minMemorySpace\": 1024,\n" +
                "\t\"image\": \"10.78.5.103:3000/projecttmp/bag_split:1.1\",\n" +
                "\t\"imagePullPolicy\": \"IfNotPresent\",\n" +
                "\t\"command\": \"[\\\"python3\\\", \\\"bag_split.py\\\"]\",\n" +
                "\t\"args\": \"[\\\"/data/input\\\",\\\"/data/output\\\"]\",\n" +
                "\t\"k8sJobType\": \"\",\n" +
                "\t\"fetchInfos\": [\"{\\\"fetchDataVolume\\\":\\\"/mnt/k8s_volume/14912393717952/fetch/\\\",\\\"fetchDataVolumeArgs\\\":\\\"[\\\"minio\\\",\\\"http://10.78.5.103:9991\\\",\\\"V0M2NPhCfk1exMSxAbnI\\\",\\\"5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX\\\",\\\"defect-data\\\",\\\"original_data/雷达鬼影/自车50kph_对象来车20kph_雷达鬼影.bag\\\",\\\"/app/downloads\\\",]\\\",\\\"fetchType\\\":\\\"minio\\\",\\\"fetchName\\\":\\\"自车50kph_对象来车20kph_雷达鬼影.bag\\\",\\\"fetchId\\\":\\\"1839476592933990400\\\"}\", \"{\\\"fetchDataVolume\\\":\\\"/mnt/k8s_volume/14912393717952/fetch/\\\",\\\"fetchDataVolumeArgs\\\":\\\"[\\\"minio\\\",\\\"http://10.78.5.103:9991\\\",\\\"V0M2NPhCfk1exMSxAbnI\\\",\\\"5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX\\\",\\\"defect-data\\\",\\\"original_data/雷达鬼影/自>车40kph_对象来车20kph_雷达鬼影.bag\\\",\\\"/app/downloads\\\",]\\\",\\\"fetchType\\\":\\\"minio\\\",\\\"fetchName\\\":\\\"自车40kph_对象来车20kph_雷达鬼影.bag\\\",\\\"fetchId\\\":\\\"1839476592791384064\\\"}\", \"{\\\"fetchDataVolume\\\":\\\"/mnt/k8s_volume/14912393717952/fetch/\\\",\\\"fetchDataVolumeArgs\\\":\\\"[\\\"minio\\\",\\\"http://10.78.5.103:9991\\\",\\\"V0M2NPhCfk1exMSxAbnI\\\",\\\"5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX\\\",\\\"defect-data\\\",\\\"original_data/雷达鬼影/自车30kph_对象来车20kph_雷>达鬼影.bag\\\",\\\"/app/downloads\\\",]\\\",\\\"fetchType\\\":\\\"minio\\\",\\\"fetchName\\\":\\\"自车30kph_对象来车20kph_雷达鬼影.bag\\\",\\\"fetchId\\\":\\\"1839476592615223296\\\"}\"],\n" +
                "\t\"dirId\": {\n" +
                "\t\t\"label\": \"车端1\",\n" +
                "\t\t\"type\": 0,\n" +
                "\t\t\"value\": \"1820284303331176448\",\n" +
                "\t\t\"isLeaf\": false,\n" +
                "\t\t\"disabled\": true,\n" +
                "\t\t\"children\": [{\n" +
                "\t\t\t\"label\": \"20231106_174710-20231106_174723.bag\",\n" +
                "\t\t\t\"value\": \"{\\\"fetchDataVolume\\\":\\\"/mnt/k8s_volume/14912393717952/fetch/\\\",\\\"fetchDataVolumeArgs\\\":\\\"[\\\"minio\\\",\\\"http://10.78.5.103:9991\\\",\\\"V0M2NPhCfk1exMSxAbnI\\\",\\\"5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX\\\",\\\"defect-data\\\",\\\"original_data/车端1/20231106_174710-20231106_174723.bag\\\",\\\"/app/downloads\\\",]\\\",\\\"fetchType\\\":\\\"minio\\\",\\\"fetchName\\\":\\\"20231106_174710-20231106_174723.bag\\\",\\\"fetchId\\\":\\\"1849273548575477760\\\"}\",\n" +
                "\t\t\t\"children\": null\n" +
                "\t\t}, {\n" +
                "\t\t\t\"label\": \"20240124_172651-20240124_172705.bag\",\n" +
                "\t\t\t\"value\": \"{\\\"fetchDataVolume\\\":\\\"/mnt/k8s_volume/14912393717952/fetch/\\\",\\\"fetchDataVolumeArgs\\\":\\\"[\\\"minio\\\",\\\"http://10.78.5.103:9991\\\",\\\"V0M2NPhCfk1exMSxAbnI\\\",\\\"5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX\\\",\\\"defect-data\\\",\\\"original_data/车端1/20240124_172651-20240124_172705.bag\\\",\\\"/app/downloads\\\",]\\\",\\\"fetchType\\\":\\\"minio\\\",\\\"fetchName\\\":\\\"20240124_172651-20240124_172705.bag\\\",\\\"fetchId\\\":\\\"1820287656740139008\\\"}\",\n" +
                "\t\t\t\"children\": null\n" +
                "\t\t}]\n" +
                "\t},\n" +
                "\t\"outputVolumeId\": \"1849364727276220416\",\n" +
                "\t\"outputVolume\": \"{\\\"outputVolumeNameInfo\\\":\\\"{\\\"name\\\":\\\"切片数据集-1849364265491738624\\\",\\\"type\\\":\\\"minio\\\",\\\"id\\\":\\\"1849364727276220416\\\",\\\"host\\\":\\\"http://10.78.5.103:9991\\\",\\\"appKey\\\":\\\"h4tel1uytOgXHDN1gGqb\\\",\\\"appSecret\\\":\\\"i2wkQIntltOu143UUiUwtz2YxP7dwJ3MukHgY6Y1\\\",\\\"bucketName\\\":\\\"tp-dataset-bucket\\\",\\\"filePath\\\":\\\"切片数据集-1849364265491738624/\\\",\\\"projectIds\\\":null,\\\"tpDatasetId\\\":\\\"1849364265491738624\\\",\\\"children\\\":null}\\\",\\\"outputVolumeId\\\":\\\"1849364727276220416\\\",\\\"outputVolumeName\\\":\\\"切片数据集-1849364265491738624\\\",\\\"dataType\\\":\\\"0\\\"}\",\n" +
                "\t\"outputVolumeNameInfo\": \"{\\\"name\\\":\\\"切片数据集-1849364265491738624\\\",\\\"type\\\":\\\"minio\\\",\\\"id\\\":\\\"1849364727276220416\\\",\\\"host\\\":\\\"http://10.78.5.103:9991\\\",\\\"appKey\\\":\\\"h4tel1uytOgXHDN1gGqb\\\",\\\"appSecret\\\":\\\"i2wkQIntltOu143UUiUwtz2YxP7dwJ3MukHgY6Y1\\\",\\\"bucketName\\\":\\\"tp-dataset-bucket\\\",\\\"filePath\\\":\\\"切片数据集-1849364265491738624/\\\",\\\"projectIds\\\":null,\\\"tpDatasetId\\\":\\\"1849364265491738624\\\",\\\"children\\\":null}\",\n" +
                "\t\"dataType\": \"0\",\n" +
                "\t\"queue\": \"projecttmp\"\n" +
                "}";
       JSONUtils.parseObject(aa, DataSetK8sTaskParameters.class);
    }


}
