package acp.lambdaS3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

import software.amazon.awssdk.services.s3.S3SystemSetting;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.logging.Handler;
import java.util.stream.Collectors;

public class TextTransformer implements RequestHandler<S3Event, String> {

    private static final Logger logger = LoggerFactory.getLogger(Handler.class);

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        String tgtBucket = System.getenv().get("COPY_DESTINATION_BUCKET");
        if (tgtBucket == null) {
            tgtBucket = "s3://acp-storage-service-copy";
            logger.error("target bucket was not set - using default: " + tgtBucket);
        }

        S3EventNotificationRecord record = s3event.getRecords().getFirst();
        String srcBucket = record.getS3().getBucket().getName();
        String srcKey = record.getS3().getObject().getUrlDecodedKey();

        logger.info("Copying " + srcBucket + "/" + srcKey + " to:  " + tgtBucket + "/" + srcKey);

        try (S3Client s3Client = S3Client.builder().build()) {
//            for (var setting : S3SystemSetting.values()) {
//                setting.environmentVariable().equals()
//            }


            String finalTgtBucket = tgtBucket;
            s3Client.copyObject(builder -> builder
                    .sourceBucket(srcBucket)
                    .sourceKey(srcKey)
                    .destinationBucket(finalTgtBucket)
                    .destinationKey(srcKey)
            );

            logger.info("Successfully copied " + srcBucket + "/" + srcKey + " to:  " + tgtBucket + "/" + srcKey);
        }

        return "Done";
    }
}
