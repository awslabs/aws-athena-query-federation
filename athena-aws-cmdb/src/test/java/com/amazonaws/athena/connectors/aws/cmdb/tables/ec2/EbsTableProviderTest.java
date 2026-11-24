/*-
 * #%L
 * athena-aws-cmdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.aws.cmdb.tables.ec2;

import com.amazonaws.athena.connectors.aws.cmdb.tables.AbstractTableProviderTest;
import com.amazonaws.athena.connectors.aws.cmdb.tables.TableProvider;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeVolumesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeVolumesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.Volume;
import software.amazon.awssdk.services.ec2.model.VolumeAttachment;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EbsTableProviderTest
        extends AbstractTableProviderTest
{
    @Mock
    private Ec2Client mockEc2;

    protected String getIdField()
    {
        return "id";
    }

    protected String getIdValue()
    {
        return "123";
    }

    protected String getExpectedSchema()
    {
        return "ec2";
    }

    protected String getExpectedTable()
    {
        return "ebs_volumes";
    }

    protected int getExpectedRows()
    {
        return 2;
    }

    protected TableProvider setUpSource()
    {
        return new EbsTableProvider(mockEc2);
    }

    @Override
    protected void setUpRead()
    {
        when(mockEc2.describeVolumes(nullable(DescribeVolumesRequest.class))).thenAnswer((InvocationOnMock invocation) -> {
            DescribeVolumesRequest request = (DescribeVolumesRequest) invocation.getArguments()[0];

            assertEquals(getIdValue(), request.volumeIds().get(0));

            List<Volume> values = new ArrayList<>();
            values.add(makeVolume(getIdValue()));
            values.add(makeVolume(getIdValue()));
            values.add(makeVolume("fake-id"));
            return DescribeVolumesResponse.builder().volumes(values).build();
        });
    }

    private Volume makeVolume(String id)
    {
        Volume volume = Volume.builder()
                .volumeId(id)
                .volumeType("type")
                .attachments(VolumeAttachment.builder()
                        .instanceId("target")
                        .device("attached_device")
                        .state("attachment_state")
                        .attachTime(new Date(100_000).toInstant()).build())
                .availabilityZone("availability_zone")
                .createTime(new Date(100_000).toInstant())
                .encrypted(true)
                .kmsKeyId("kms_key_id")
                .size(100)
                .iops(100)
                .snapshotId("snapshot_id")
                .state("state")
                .tags(Tag.builder().key("key").value("value").build()).build();

        return volume;
    }
}
