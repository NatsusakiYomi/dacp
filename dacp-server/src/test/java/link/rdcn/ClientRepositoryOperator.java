package link.rdcn;

import cn.cnic.operatordownload.client.OperatorClient;
import cn.cnic.operatordownload.model.PackageInfo;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 使用示例演示如何在其他项目中使用OperatorDownloadClient
 */
public class ClientRepositoryOperator {

    public static void main(String[] args) {
        // 创建客户端实例，指向下载服务的地址
        // token: 用户认证,默认填null先
        OperatorClient client = OperatorClient.connect("http://10.0.89.39:8090", null);
//        OperatorClient client = OperatorClient.connect("http://localhost:8088", null);

        try {
            // 示例1: 上传算子
            System.out.println("=== 上传算子 ===");
            // 创建测试文件
            Path tempFile = Files.createTempFile("test-operator", ".whl");
            Files.write(tempFile, "this is a test operator file".getBytes());

            Path requirementsFile = Files.createTempFile("requirements", ".txt");
            Files.write(requirementsFile, "numpy==1.21.0\npandas>=1.3.0".getBytes());

            Path paramFile = Files.createTempFile("param", ".json");
            Files.write(paramFile, "[{\"number\": 1, \"name\": \"input\", \"paramType\": \"输入文件\"}]".getBytes());

            String uploadResult = client.uploadOperator(
                    tempFile.toFile(),          // file 算子文件
                    requirementsFile.toFile(),  // requirementsFile 依赖文件
                    paramFile.toFile(),         // paramFile 参数文件
                    "test-operator",            // name 算子名称
                    "1.0.0",                    // version 版本号
                    "测试算子描述",                // description 描述
                    "测试作者",                  // author 作者
                    "/image/testClient",           // categoryPath 目录路径
                    "PYTHON_IMAGE",                   // targetType 上传目标类型
                    "python-script",            // type 算子类型
                    "python",                   // language 编程语言
                    "pandas",                   // framework 框架
                    "python main.py",           // command 执行命令
                    "帮助信息",                  // help 帮助信息
                    "test@example.com",         // email 邮箱
                    "faird/main.py",                  // entryPoint 入口点
                    "Test Operator"             // nameEn 英文名称
            );

            System.out.println("算子上传成功: " + uploadResult);

            // 清理临时文件
            Files.delete(tempFile);
            Files.delete(requirementsFile);
            Files.delete(paramFile);

            // 示例2: 根据名称和版本查询算子信息
            System.out.println("\n=== 查询算子信息 ===");
            String operatorInfo = client.getOperatorByNameAndVersion("test-operator", "1.0.0");
            System.out.println("算子信息: " + operatorInfo);

            // 示例3: 获取文件信息
//            System.out.println("\n=== 获取文件信息 ===");
//            PackageInfo packageInfo = client.getPackageInfo("test-upload-id", "2.0.0");
//            if (packageInfo != null) {
//                System.out.println("文件ID: " + packageInfo.getId());
//                System.out.println("文件名: " + packageInfo.getPackageName());
//                System.out.println("文件类型: " + packageInfo.getType());
//                System.out.println("描述: " + packageInfo.getDesc());
//                System.out.println("功能名称: " + packageInfo.getFunctionName());
//                System.out.println("版本: " + packageInfo.getVersion());
//            }

//            // 示例4: 下载文件
//            System.out.println("\n=== 下载文件 ===");
//            byte[] fileData = client.downloadPackage("test-upload-id", "2.0.0");
//            if (fileData != null) {
//                System.out.println("成功下载文件，大小: " + fileData.length + " 字节");
//            }

            // 示例5: 上传文件 (需要有一个实际存在的文件)
            System.out.println("\n=== 上传文件 ===");
            // 创建一个临时文件用于测试
            Path tempFile2 = Files.createTempFile("test-upload", ".txt");
            Files.write(tempFile2, "this is a test file".getBytes());

            PackageInfo uploadPackage = client.uploadPackage(
                    "test-upload-id",           // id
                    "2.0.0",                    // version
                    tempFile2.toFile(),         // file
                    "txt",                      // type
                    "testDesc",                 // desc
                    "test.upload"               // functionName
            );

            System.out.println("上传成功: " + uploadPackage);

            // 清理临时文件
            Files.delete(tempFile2);

        } catch (Exception e) {
            System.err.println("操作失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
