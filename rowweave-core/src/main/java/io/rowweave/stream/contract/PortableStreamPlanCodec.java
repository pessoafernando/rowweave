package io.rowweave.stream.contract;

import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamExpression;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Canonical, checksummed and bounded PortableStreamPlan v1 frame. */
public final class PortableStreamPlanCodec {
    private static final byte[] MAGIC_V1="RWXXPSP1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V2="RWXXPSP2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V3="RWXXPSP3".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES=16,DIGEST_BYTES=32,MAX_BODY_BYTES=32*1024*1024;
    public static final int MAX_FRAME_BYTES=HEADER_BYTES+MAX_BODY_BYTES+DIGEST_BYTES;
    private PortableStreamPlanCodec(){}
    public static byte[] magic(){return MAGIC_V1.clone();}

    public static byte[] encode(PortableStreamPlan plan){
        if(plan==null)throw new IllegalArgumentException("plan is required");
        try{ByteArrayOutputStream bodyBytes=new ByteArrayOutputStream();try(DataOutputStream out=new DataOutputStream(bodyBytes)){
            out.writeInt(plan.planVersion());out.writeInt(plan.deliveryGuarantee().wireId());out.writeInt(plan.parallelism());out.writeInt(plan.maxParallelism());out.writeInt(plan.changelogMode().wireId());out.writeInt(plan.timeMode().wireId());writeTexts(out,plan.requiredCapabilities());writeTexts(out,plan.rootSinkOperatorUids());out.writeInt(plan.edges().size());boolean tagged=plan.requiredCapabilities().contains(PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY);boolean nodeExecution=plan.requiredCapabilities().contains(PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY);boolean dynamicSource=plan.planVersion()>=PortableStreamPlan.DYNAMIC_SOURCE_VERSION;boolean retract=plan.planVersion()>=PortableStreamPlan.RETRACT_CHANGELOG_VERSION;for(StreamEdge edge:plan.edges())writeEdge(out,edge,tagged);out.writeInt(plan.nodes().size());for(PortableStreamNode n:plan.nodes())writeNode(out,n,tagged,nodeExecution,dynamicSource,retract);
        }byte[] body=bodyBytes.toByteArray();if(body.length>MAX_BODY_BYTES)throw fail("LIMIT_EXCEEDED","envelope.body","body exceeds limit");ByteArrayOutputStream frame=new ByteArrayOutputStream(HEADER_BYTES+body.length+DIGEST_BYTES);try(DataOutputStream out=new DataOutputStream(frame)){out.write(plan.planVersion()>=PortableStreamPlan.RETRACT_CHANGELOG_VERSION?MAGIC_V3:plan.planVersion()>=PortableStreamPlan.DYNAMIC_SOURCE_VERSION?MAGIC_V2:MAGIC_V1);out.writeInt(plan.planVersion());out.writeInt(body.length);out.write(body);out.write(sha256(body));}return frame.toByteArray();}catch(IOException impossible){throw new IllegalStateException(impossible);}
    }
    private static void writeEdge(DataOutputStream out, StreamEdge edge,
            boolean tagged)
            throws IOException {
        writeText(out, edge.sourceOperatorUid());
        writeText(out, edge.targetOperatorUid());
        out.writeInt(edge.targetInputIndex());
        writeText(out, edge.logicalChannelId());
        out.writeInt(edge.partitioning().wireId());
        out.writeInt(edge.maximumFrameBytes());
        out.writeInt(edge.initialCreditBytes());
        if (tagged) {
            out.writeBoolean(edge.sourceOutputTagId() != null);
            if (edge.sourceOutputTagId() != null) {
                writeText(out, edge.sourceOutputTagId());
            }
        }
    }
    private static void writeNode(DataOutputStream out, PortableStreamNode node,
            boolean tagged, boolean nodeExecution,
            boolean dynamicSourceEncoding, boolean retractEncoding)
            throws IOException {
        writeText(out, node.operatorUid());
        out.writeInt(node.opcode().wireId());
        writeTexts(out, node.inputOperatorUids());
        writeTexts(out, node.inputSchemaFingerprints());
        writeSchema(out, node.outputSchema());
        writeTexts(out, node.stableKeyFields());
        if (nodeExecution) {
            StreamNodeExecutionDescriptor execution =
                    node.executionDescriptor();
            out.writeInt(execution.changelogMode().wireId());
            out.writeInt(execution.timeMode().wireId());
            out.writeInt(execution.parallelism());
            out.writeInt(execution.maximumParallelism());
            out.writeLong(execution.maximumResidentBytes());
            out.writeInt(execution.maximumTimerCount());
            out.writeInt(execution.maximumWindowCount());
            out.writeLong(execution.maximumCheckpointBytes());
            out.writeLong(execution.maximumSpillDiskBytes());
            out.writeInt(execution.maximumThreads());
        }
        out.writeInt(node.stateDescriptors().size());
        for (StreamStateDescriptor state : node.stateDescriptors()) {
            writeText(out, state.stateName());
            out.writeInt(state.kind().wireId());
            writeText(out, state.serializerContractId());
            out.writeInt(state.stateSchemaVersion());
            out.writeInt(state.ttlMode().wireId());
            out.writeLong(state.ttlMillis());
        }
        out.writeInt(node.scalarParameters().size());
        for (Map.Entry<String, String> entry : node.scalarParameters().entrySet()) {
            writeText(out, entry.getKey());
            writeText(out, entry.getValue());
        }
        StreamNodeBinding binding = node.binding();
        if (binding == null) {
            out.writeInt(0);
        } else if (binding instanceof StreamSourceBinding) {
            StreamSourceBinding source = (StreamSourceBinding) binding;
            out.writeInt(1);
            writeText(out, source.providerKey());
            writeText(out, source.providerContractDigest());
            writeText(out, source.configurationDigest());
            writeFrame(out, source.configurationFrameUnsafe());
            if (dynamicSourceEncoding) {
                out.writeBoolean(source.dynamicDiscovery());
            }
            if (source.dynamicDiscovery()) {
                StreamSourceDiscoveryDescriptor discovery =
                        source.discoveryDescriptor();
                out.writeLong(discovery.discoveryIntervalMillis());
                out.writeLong(discovery.discoveryTimeoutMillis());
                out.writeInt(discovery.maximumKnownSplits());
                out.writeInt(discovery.maximumPendingSplits());
                out.writeInt(discovery.maximumChangesPerDiscovery());
                out.writeInt(discovery.maximumCursorBytes());
                out.writeInt(discovery.maximumDescriptorBytes());
            } else {
                out.writeInt(source.fixedSplits().size());
                for (StreamSourceSplit split : source.fixedSplits()) {
                    writeText(out, split.splitId());
                    writeText(out, split.positionCodecId());
                    writeFrame(out, split.descriptorFrameUnsafe());
                }
            }
            out.writeInt(source.replayGuarantee().wireId());
        } else if (binding instanceof StreamSinkBinding) {
            StreamSinkBinding sink = (StreamSinkBinding) binding;
            out.writeInt(2);
            writeText(out, sink.providerKey());
            writeText(out, sink.providerContractDigest());
            writeText(out, sink.configurationDigest());
            writeFrame(out, sink.configurationFrameUnsafe());
            out.writeInt(sink.visibilityProfile().wireId());
            if (retractEncoding) {
                out.writeInt(sink.acceptedChangelogModes().size());
                for (PortableStreamPlan.ChangelogMode mode
                        : sink.acceptedChangelogModes()) {
                    out.writeInt(mode.wireId());
                }
            }
        } else if (binding instanceof StreamFunctionBinding) {
            StreamFunctionBinding function = (StreamFunctionBinding) binding;
            FunctionDeploymentReference deployment = function.deploymentReference();
            out.writeInt(3);
            out.writeInt(function.functionKind().wireId());
            writeText(out, deployment.deploymentSetId());
            writeText(out, deployment.deploymentId());
            writeText(out, deployment.deploymentJarDigest());
            writeText(out, deployment.className());
            out.writeInt(deployment.functionAbi().wireId());
            writeText(out, deployment.configurationCodecId());
            writeText(out, deployment.configurationDigest());
            writeFrame(out, deployment.configurationFrame());
            out.writeLong(deployment.resourceProfile().maximumHeapBytes());
            out.writeLong(deployment.resourceProfile().maximumDirectBytes());
            out.writeInt(deployment.resourceProfile().maximumOutputRowsPerInput());
        } else if (binding instanceof StreamCalcBinding) {
            StreamCalcBinding calc = (StreamCalcBinding) binding;
            out.writeInt(4);
            out.writeBoolean(calc.predicate() != null);
            if (calc.predicate() != null) {
                writeFrame(out, StreamExpressionCodec.encode(calc.predicate()));
            }
            out.writeInt(calc.projections().size());
            for (StreamCalcBinding.Projection projection :
                    calc.projections()) {
                writeText(out, projection.outputFieldId());
                out.writeBoolean(projection.decimalOutputBoundary());
                writeFrame(out, StreamExpressionCodec.encode(
                        projection.expression()));
            }
        } else if (binding instanceof StreamTimeBinding) {
            StreamTimeBinding time = (StreamTimeBinding) binding;
            out.writeInt(5);
            writeFrame(out, StreamExpressionCodec.encode(
                    time.timestampExpression()));
            out.writeInt(time.watermarkMode().wireId());
            out.writeLong(time.maximumOutOfOrderMillis());
            out.writeLong(time.idleTimeoutMillis());
            out.writeInt(time.invalidTimestampPolicy().wireId());
        } else if (binding instanceof StreamProcessBinding) {
            StreamProcessBinding process = (StreamProcessBinding) binding;
            FunctionDeploymentReference deployment =
                    process.deploymentReference();
            out.writeInt(6);
            out.writeInt(process.functionKind().wireId());
            writeText(out, deployment.deploymentSetId());
            writeText(out, deployment.deploymentId());
            writeText(out, deployment.deploymentJarDigest());
            writeText(out, deployment.className());
            out.writeInt(deployment.functionAbi().wireId());
            writeText(out, deployment.configurationCodecId());
            writeText(out, deployment.configurationDigest());
            writeFrame(out, deployment.configurationFrame());
            out.writeLong(deployment.resourceProfile().maximumHeapBytes());
            out.writeLong(deployment.resourceProfile().maximumDirectBytes());
            out.writeInt(deployment.resourceProfile()
                    .maximumOutputRowsPerInput());
            out.writeInt(process.managedStates().size());
            for (StreamManagedStateDescriptor state :
                    process.managedStates()) {
                writeText(out, state.state().stateName());
                out.writeInt(state.maximumEntriesPerKey());
                out.writeLong(state.maximumTotalEntries());
                out.writeLong(state.maximumTotalBytes());
                out.writeLong(state.maximumEntryBytes());
            }
            out.writeInt(process.timerFamilies().size());
            for (StreamTimerDescriptor timer : process.timerFamilies()) {
                writeText(out, timer.timerFamilyId());
                out.writeInt(timer.timeDomain().wireId());
                out.writeInt(timer.maximumTimersPerKey());
                out.writeLong(timer.maximumTotalTimers());
                out.writeLong(timer.maximumTotalBytes());
            }
            out.writeInt(process.maximumElementOutputs());
            out.writeLong(process.maximumElementOutputBytes());
            out.writeInt(process.maximumRightOutputs());
            out.writeLong(process.maximumRightOutputBytes());
            out.writeInt(process.maximumTimerOutputs());
            out.writeLong(process.maximumTimerOutputBytes());
            StreamBroadcastStateDescriptor broadcast =
                    process.broadcastState();
            out.writeBoolean(broadcast != null);
            if (broadcast != null) {
                writeText(out, broadcast.stateName());
                out.writeInt(broadcast.authorityKind().wireId());
                writeText(out, broadcast.snapshotOrVersionId());
                writeText(out, broadcast.authorityDigest());
                writeText(out, broadcast.serializerContractId());
                out.writeInt(broadcast.stateSchemaVersion());
                out.writeLong(broadcast.maximumEntries());
                out.writeLong(broadcast.maximumTotalBytes());
                out.writeLong(broadcast.maximumUpdateBytes());
                out.writeInt(broadcast.maximumRetainedVersions());
            }
            if (tagged) {
                out.writeInt(process.outputTags().size());
                for (StreamOutputTagDescriptor tag : process.outputTags()) {
                    writeText(out, tag.tagId());
                    writeSchema(out, tag.schema());
                    out.writeInt(tag.changelogMode().wireId());
                    writeTexts(out, tag.stableKeyFields());
                    out.writeInt(tag.maximumLeftOutputs());
                    out.writeLong(tag.maximumLeftOutputBytes());
                    out.writeInt(tag.maximumRightOutputs());
                    out.writeLong(tag.maximumRightOutputBytes());
                    out.writeInt(tag.maximumTimerOutputs());
                    out.writeLong(tag.maximumTimerOutputBytes());
                }
            }
        } else if (binding instanceof StreamWindowBinding) {
            StreamWindowBinding window = (StreamWindowBinding) binding;
            FunctionDeploymentReference deployment =
                    window.deploymentReference();
            out.writeInt(7);
            out.writeInt(window.assignerKind().wireId());
            out.writeBoolean(window.eventTimeFieldId() != null);
            if (window.eventTimeFieldId() != null) {
                writeText(out, window.eventTimeFieldId());
            }
            out.writeLong(window.sizeOrGap());
            out.writeLong(window.slide());
            out.writeInt(window.triggerKind().wireId());
            out.writeLong(window.triggerIntervalOrCount());
            out.writeBoolean(window.purging());
            out.writeInt(window.accumulationMode().wireId());
            out.writeLong(window.allowedLatenessMillis());
            out.writeInt(window.functionKind().wireId());
            writeText(out, deployment.deploymentSetId());
            writeText(out, deployment.deploymentId());
            writeText(out, deployment.deploymentJarDigest());
            writeText(out, deployment.className());
            out.writeInt(deployment.functionAbi().wireId());
            writeText(out, deployment.configurationCodecId());
            writeText(out, deployment.configurationDigest());
            writeFrame(out, deployment.configurationFrame());
            out.writeLong(deployment.resourceProfile().maximumHeapBytes());
            out.writeLong(deployment.resourceProfile().maximumDirectBytes());
            out.writeInt(deployment.resourceProfile()
                    .maximumOutputRowsPerInput());
            StreamWindowResourceDescriptor resources = window.resources();
            out.writeInt(resources.maximumActiveWindowsPerKey());
            out.writeLong(resources.maximumRowsPerWindow());
            out.writeLong(resources.maximumWindowBytes());
            out.writeLong(resources.maximumTotalBytes());
            out.writeInt(resources.maximumSessionMergeFanIn());
            out.writeInt(resources.maximumTriggerOutputs());
            out.writeLong(resources.maximumTriggerOutputBytes());
            if (tagged) {
                out.writeBoolean(window.lateOutputTag() != null);
                if (window.lateOutputTag() != null) {
                    StreamOutputTagDescriptor tag = window.lateOutputTag();
                    writeText(out, tag.tagId());
                    writeSchema(out, tag.schema());
                    out.writeInt(tag.changelogMode().wireId());
                    writeTexts(out, tag.stableKeyFields());
                    out.writeInt(tag.maximumLeftOutputs());
                    out.writeLong(tag.maximumLeftOutputBytes());
                    out.writeInt(tag.maximumRightOutputs());
                    out.writeLong(tag.maximumRightOutputBytes());
                    out.writeInt(tag.maximumTimerOutputs());
                    out.writeLong(tag.maximumTimerOutputBytes());
                }
            }
        } else if (binding instanceof StreamAsyncBinding) {
            StreamAsyncBinding async = (StreamAsyncBinding) binding;
            FunctionDeploymentReference deployment =
                    async.deploymentReference();
            out.writeInt(8);
            out.writeInt(async.ordering().wireId());
            out.writeInt(async.replayMode().wireId());
            out.writeBoolean(async.replayAuthorityId() != null);
            if (async.replayAuthorityId() != null) {
                writeText(out, async.replayAuthorityId());
                writeText(out, async.replayAuthorityDigest());
            }
            writeText(out, deployment.deploymentSetId());
            writeText(out, deployment.deploymentId());
            writeText(out, deployment.deploymentJarDigest());
            writeText(out, deployment.className());
            out.writeInt(deployment.functionAbi().wireId());
            writeText(out, deployment.configurationCodecId());
            writeText(out, deployment.configurationDigest());
            writeFrame(out, deployment.configurationFrame());
            out.writeLong(deployment.resourceProfile().maximumHeapBytes());
            out.writeLong(deployment.resourceProfile().maximumDirectBytes());
            out.writeInt(deployment.resourceProfile()
                    .maximumOutputRowsPerInput());
            StreamAsyncResourceDescriptor resources = async.resources();
            out.writeInt(resources.maximumConcurrency());
            out.writeLong(resources.maximumPendingBytes());
            out.writeLong(resources.timeoutMillis());
            out.writeInt(resources.maximumAttempts());
            out.writeLong(resources.initialBackoffMillis());
            out.writeLong(resources.maximumBackoffMillis());
            out.writeInt(resources.maximumResults());
            out.writeLong(resources.maximumResultBytes());
        } else {
            throw fail("BINDING_KIND_MISMATCH", "nodes.binding",
                    "unknown binding implementation");
        }
    }
    private static void writeSchema(DataOutputStream out,PortableRowSchema schema)throws IOException{out.writeInt(schema.fieldCount());for(PortableField f:schema.fields()){writeText(out,f.fieldId());writeText(out,f.name());writeText(out,f.alias());out.writeInt(f.dataType().wireId());out.writeBoolean(f.nullable());}}
    private static void writeTexts(DataOutputStream out,List<String> values)throws IOException{out.writeInt(values.size());for(String v:values)writeText(out,v);}private static void writeText(DataOutputStream out,String value)throws IOException{byte[] b=value.getBytes(StandardCharsets.UTF_8);out.writeInt(b.length);out.write(b);}private static void writeFrame(DataOutputStream out,byte[] value)throws IOException{out.writeInt(value.length);out.write(value);}

    public static PortableStreamPlan decode(byte[] frame){
        return decode(frame, false);
    }

    /** Frozen v1 reader used for N-1 compatibility and fail-closed tests. */
    public static PortableStreamPlan decodeV1(byte[] frame) {
        return decode(frame, true);
    }

    private static PortableStreamPlan decode(byte[] frame, boolean v1Only){
        if(frame==null||frame.length<HEADER_BYTES+DIGEST_BYTES)throw fail("FRAME_LENGTH_MISMATCH","envelope","frame is too short");if(frame.length>MAX_FRAME_BYTES)throw fail("LIMIT_EXCEEDED","envelope","frame exceeds limit");
        try(DataInputStream in=new DataInputStream(new ByteArrayInputStream(frame))){byte[] magic=new byte[MAGIC_V1.length];in.readFully(magic);boolean v1=MessageDigest.isEqual(magic,MAGIC_V1);boolean v2=MessageDigest.isEqual(magic,MAGIC_V2);boolean v3=MessageDigest.isEqual(magic,MAGIC_V3);if(!v1&&!v2&&!v3)throw fail("INVALID_MAGIC","envelope.magic","invalid magic");if(v1Only&&!v1)throw fail("UNSUPPORTED_ENVELOPE_VERSION","envelope.version","Stream plan v1 reader rejects newer capabilities");int envelopeVersion=in.readInt();if((v1&&envelopeVersion!=PortableStreamPlan.FIXED_SOURCE_VERSION)||(v2&&envelopeVersion!=PortableStreamPlan.DYNAMIC_SOURCE_VERSION)||(v3&&envelopeVersion!=PortableStreamPlan.RETRACT_CHANGELOG_VERSION))throw fail("UNSUPPORTED_ENVELOPE_VERSION","envelope.version","unsupported envelope version");int length=in.readInt();if(length<0||length>MAX_BODY_BYTES||HEADER_BYTES+(long)length+DIGEST_BYTES!=frame.length)throw fail("FRAME_LENGTH_MISMATCH","envelope.body","invalid body length");byte[] actual=sha256(frame,HEADER_BYTES,length),expected=Arrays.copyOfRange(frame,HEADER_BYTES+length,frame.length);if(!MessageDigest.isEqual(actual,expected))throw fail("CHECKSUM_MISMATCH","envelope.digest","checksum mismatch");try(DataInputStream body=new DataInputStream(new ByteArrayInputStream(frame,HEADER_BYTES,length))){PortableStreamPlan plan=readPlan(body,envelopeVersion);if(body.available()!=0)throw fail("FRAME_LENGTH_MISMATCH","envelope.body","trailing body bytes");return plan;}
        }catch(StreamContractException e){throw e;}catch(EOFException e){throw fail("FRAME_LENGTH_MISMATCH","envelope.body","truncated body");}catch(IOException e){throw fail("FRAME_LENGTH_MISMATCH","envelope.body","malformed body");}
    }
    private static PortableStreamPlan readPlan(DataInputStream in,int envelopeVersion)throws IOException{int version=in.readInt();if(version!=envelopeVersion)throw fail("UNSUPPORTED_PLAN_VERSION","plan.version","plan and envelope versions differ");PortableStreamPlan.DeliveryGuarantee delivery=PortableStreamPlan.DeliveryGuarantee.fromWire(in.readInt());int p=in.readInt(),mp=in.readInt();PortableStreamPlan.ChangelogMode changelog=PortableStreamPlan.ChangelogMode.fromWire(in.readInt());PortableStreamPlan.TimeMode time=PortableStreamPlan.TimeMode.fromWire(in.readInt());List<String> caps=readTexts(in,1,256,"plan.capabilities");boolean tagged=caps.contains(PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY);boolean nodeExecution=caps.contains(PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY);boolean dynamicSource=version>=PortableStreamPlan.DYNAMIC_SOURCE_VERSION;boolean retract=version>=PortableStreamPlan.RETRACT_CHANGELOG_VERSION;List<String> roots=readTexts(in,1,64,"plan.roots");List<StreamEdge> edges=readEdges(in,tagged);int count=readCount(in,1,PortableStreamPlan.MAX_NODES,"plan.nodes");List<PortableStreamNode> nodes=new ArrayList<>(count);for(int i=0;i<count;i++)nodes.add(readNode(in,tagged,nodeExecution,dynamicSource,retract));return new PortableStreamPlan(version,delivery,p,mp,changelog,time,roots,nodes,edges,caps);}
    private static List<StreamEdge> readEdges(DataInputStream in,
            boolean tagged) throws IOException {
        int count = readCount(in, 0, PortableStreamPlan.MAX_NODES * 2,
                "plan.edges");
        List<StreamEdge> edges = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String source = readText(in, 256,
                    "plan.edges.sourceOperatorUid");
            String target = readText(in, 256,
                    "plan.edges.targetOperatorUid");
            int input = in.readInt();
            String channel = readText(in, 256,
                    "plan.edges.logicalChannelId");
            StreamEdge.Partitioning partitioning =
                    StreamEdge.Partitioning.fromWire(in.readInt());
            int frameBytes = in.readInt();
            int creditBytes = in.readInt();
            String outputTag = tagged && in.readBoolean()
                    ? readText(in, 256, "plan.edges.sourceOutputTagId")
                    : null;
            edges.add(new StreamEdge(source, target, input, channel,
                    partitioning, frameBytes, creditBytes, outputTag));
        }
        return edges;
    }
    private static PortableStreamNode readNode(DataInputStream in,boolean tagged,boolean nodeExecution,boolean dynamicSourceEncoding,boolean retractEncoding)throws IOException{String uid=readText(in,256,"nodes.uid");StreamOperatorCode op=StreamOperatorCode.fromWire(in.readInt());List<String> inputs=readTexts(in,0,2,"nodes.inputs");List<String> schemas=readTexts(in,0,2,"nodes.inputSchemas");PortableRowSchema output=readSchema(in);List<String> keys=readTexts(in,0,1024,"nodes.keys");StreamNodeExecutionDescriptor execution=nodeExecution?new StreamNodeExecutionDescriptor(PortableStreamPlan.ChangelogMode.fromWire(in.readInt()),PortableStreamPlan.TimeMode.fromWire(in.readInt()),in.readInt(),in.readInt(),in.readLong(),in.readInt(),in.readInt(),in.readLong(),in.readLong(),in.readInt()):null;int stateCount=readCount(in,0,64,"nodes.states");List<StreamStateDescriptor> states=new ArrayList<>(stateCount);for(int i=0;i<stateCount;i++)states.add(new StreamStateDescriptor(readText(in,256,"state.name"),StreamStateDescriptor.Kind.fromWire(in.readInt()),readText(in,256,"state.serializer"),in.readInt(),StreamStateDescriptor.TtlMode.fromWire(in.readInt()),in.readLong()));int pc=readCount(in,0,64,"nodes.parameters");Map<String,String> params=new LinkedHashMap<>();for(int i=0;i<pc;i++){String key=readText(in,256,"parameter.key");if(params.put(key,readText(in,4096,"parameter.value"))!=null)throw fail("DUPLICATE_PARAMETER","nodes.parameters","duplicate parameter");}StreamNodeBinding binding=readBinding(in,states,tagged,dynamicSourceEncoding,retractEncoding);return new PortableStreamNode(uid,op,inputs,schemas,output,keys,states,params,binding,execution);}
    private static StreamNodeBinding readBinding(DataInputStream in,
            List<StreamStateDescriptor> nodeStates, boolean tagged,
            boolean dynamicSourceEncoding, boolean retractEncoding)
            throws IOException {
        int kind = in.readInt();
        if (kind == 0) return null;
        if (kind == 1) {
            String key = readText(in, 256, "source.key");
            String digest = readText(in, 64, "source.digest");
            String configurationDigest = readText(
                    in, 64, "source.configurationDigest");
            byte[] config = readFrame(in, "source.config");
            boolean dynamic = dynamicSourceEncoding && in.readBoolean();
            if (dynamic) {
                StreamSourceDiscoveryDescriptor discovery =
                        new StreamSourceDiscoveryDescriptor(
                                in.readLong(), in.readLong(), in.readInt(),
                                in.readInt(), in.readInt(), in.readInt(),
                                in.readInt());
                return new StreamSourceBinding(key, digest,
                        configurationDigest, config, discovery,
                        StreamSourceBinding.ReplayGuarantee.fromWire(
                                in.readInt()));
            }
            int count = readCount(in, 1, 4096, "source.splits");
            List<StreamSourceSplit> splits = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                splits.add(new StreamSourceSplit(
                        readText(in, 64, "source.split.splitId"),
                        readText(in, 256, "source.split.positionCodecId"),
                        readFrame(in, "source.split.descriptor")));
            }
            return new StreamSourceBinding(key, digest, configurationDigest,
                    config, splits,
                    StreamSourceBinding.ReplayGuarantee.fromWire(in.readInt()));
        }
        if (kind == 2) {
            String key = readText(in, 256, "sink.key");
            String digest = readText(in, 64, "sink.digest");
            String configurationDigest = readText(
                    in, 64, "sink.configurationDigest");
            byte[] configuration = readFrame(in, "sink.config");
            StreamSinkBinding.VisibilityProfile visibility =
                    StreamSinkBinding.VisibilityProfile.fromWire(
                            in.readInt());
            List<PortableStreamPlan.ChangelogMode> modes = Arrays.asList(
                    PortableStreamPlan.ChangelogMode.APPEND,
                    PortableStreamPlan.ChangelogMode.UPSERT);
            if (retractEncoding) {
                int count = readCount(in, 1, 3, "sink.changelogModes");
                modes = new ArrayList<>(count);
                for (int index = 0; index < count; index++) {
                    modes.add(PortableStreamPlan.ChangelogMode.fromWire(
                            in.readInt()));
                }
            }
            return new StreamSinkBinding(
                    key, digest, configurationDigest, configuration,
                    visibility, modes);
        }
        if (kind == 3) {
            StreamFunctionBinding.FunctionKind functionKind =
                    StreamFunctionBinding.FunctionKind.fromWire(in.readInt());
            String deploymentSetId = readText(in, 64, "function.deploymentSetId");
            String deploymentId = readText(in, 256, "function.deploymentId");
            String jarDigest = readText(in, 64, "function.deploymentJarDigest");
            String className = readText(in, 4096, "function.className");
            FunctionAbi abi = FunctionAbi.fromWireId(in.readInt());
            String codecId = readText(in, 256, "function.configurationCodecId");
            String configurationDigest = readText(
                    in, 64, "function.configurationDigest");
            byte[] configuration = readFrame(in, "function.configuration");
            FunctionResourceProfile resources = new FunctionResourceProfile(
                    in.readLong(), in.readLong(), in.readInt());
            return new StreamFunctionBinding(functionKind,
                    new FunctionDeploymentReference(
                            deploymentSetId, deploymentId, jarDigest, className,
                            abi, codecId, configurationDigest,
                            configuration, resources));
        }
        if (kind == 4) {
            StreamExpression predicate = in.readBoolean()
                    ? StreamExpressionCodec.decode(readFrame(in,
                    "calc.predicate")) : null;
            int count = readCount(in, 1, StreamCalcBinding.MAX_PROJECTIONS,
                    "calc.projections");
            List<StreamCalcBinding.Projection> projections =
                    new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                String fieldId = readText(in, 256,
                        "calc.projection.outputFieldId");
                boolean decimal = in.readBoolean();
                StreamExpression expression = StreamExpressionCodec.decode(
                        readFrame(in, "calc.projection.expression"));
                projections.add(decimal
                        ? StreamCalcBinding.decimalProjection(
                        fieldId, expression)
                        : StreamCalcBinding.projection(fieldId, expression));
            }
            return new StreamCalcBinding(predicate, projections);
        }
        if (kind == 5) {
            return new StreamTimeBinding(
                    StreamExpressionCodec.decode(readFrame(in,
                            "time.timestampExpression")),
                    StreamTimeBinding.WatermarkMode.fromWireId(in.readInt()),
                    in.readLong(), in.readLong(),
                    StreamTimeBinding.InvalidTimestampPolicy.fromWireId(
                            in.readInt()));
        }
        if (kind == 6) {
            StreamProcessBinding.FunctionKind functionKind =
                    StreamProcessBinding.FunctionKind.fromWireId(in.readInt());
            String deploymentSetId = readText(in, 64,
                    "process.deploymentSetId");
            String deploymentId = readText(in, 256,
                    "process.deploymentId");
            String jarDigest = readText(in, 64,
                    "process.deploymentJarDigest");
            String className = readText(in, 4096, "process.className");
            FunctionAbi abi = FunctionAbi.fromWireId(in.readInt());
            String codecId = readText(in, 256,
                    "process.configurationCodecId");
            String configurationDigest = readText(in, 64,
                    "process.configurationDigest");
            byte[] configuration = readFrame(in,
                    "process.configuration");
            FunctionDeploymentReference deployment =
                    new FunctionDeploymentReference(deploymentSetId,
                            deploymentId, jarDigest, className, abi, codecId,
                            configurationDigest, configuration,
                            new FunctionResourceProfile(in.readLong(),
                                    in.readLong(), in.readInt()));
            int stateCount = readCount(in, 0, 64,
                    "process.managedStates");
            List<StreamManagedStateDescriptor> states =
                    new ArrayList<>(stateCount);
            for (int index = 0; index < stateCount; index++) {
                String name = readText(in, 256,
                        "process.managedStates.name");
                StreamStateDescriptor state = stateByName(nodeStates, name);
                states.add(new StreamManagedStateDescriptor(state,
                        in.readInt(), in.readLong(), in.readLong(),
                        in.readLong()));
            }
            int timerCount = readCount(in, 0, 64,
                    "process.timerFamilies");
            List<StreamTimerDescriptor> timers =
                    new ArrayList<>(timerCount);
            for (int index = 0; index < timerCount; index++) {
                timers.add(new StreamTimerDescriptor(
                        readText(in, 256, "process.timer.id"),
                        StreamTimerDescriptor.TimeDomain.fromWireId(
                                in.readInt()), in.readInt(), in.readLong(),
                        in.readLong()));
            }
            int leftRows = in.readInt();
            long leftBytes = in.readLong();
            int rightRows = in.readInt();
            long rightBytes = in.readLong();
            int timerRows = in.readInt();
            long timerBytes = in.readLong();
            StreamBroadcastStateDescriptor broadcast = null;
            if (in.readBoolean()) {
                broadcast = new StreamBroadcastStateDescriptor(
                        readText(in, 256, "broadcast.stateName"),
                        StreamBroadcastStateDescriptor.AuthorityKind
                                .fromWireId(in.readInt()),
                        readText(in, 256, "broadcast.authorityId"),
                        readText(in, 64, "broadcast.authorityDigest"),
                        readText(in, 256, "broadcast.serializer"),
                        in.readInt(), in.readLong(), in.readLong(),
                        in.readLong(), in.readInt());
            }
            List<StreamOutputTagDescriptor> outputTags = Collections.emptyList();
            if (tagged) {
                int tagCount = readCount(in, 0,
                        io.rowweave.stream.api.StreamProcessSpec.MAX_OUTPUT_TAGS,
                        "process.outputTags");
                outputTags = new ArrayList<>(tagCount);
                for (int index = 0; index < tagCount; index++) {
                    outputTags.add(new StreamOutputTagDescriptor(
                            readText(in, 256, "process.outputTags.id"),
                            readSchema(in),
                            StreamOutputTagDescriptor.ChangelogMode
                                    .fromWireId(in.readInt()),
                            readTexts(in, 0, 1024,
                                    "process.outputTags.keys"),
                            in.readInt(), in.readLong(), in.readInt(),
                            in.readLong(), in.readInt(), in.readLong()));
                }
            }
            return new StreamProcessBinding(functionKind, deployment,
                    states, timers, leftRows, leftBytes, rightRows,
                    rightBytes, timerRows, timerBytes, broadcast, outputTags);
        }
        if (kind == 7) {
            StreamWindowBinding.AssignerKind assigner =
                    StreamWindowBinding.AssignerKind.fromWireId(in.readInt());
            String eventTimeField = in.readBoolean()
                    ? readText(in, 256, "window.eventTimeFieldId") : null;
            long sizeOrGap = in.readLong();
            long slide = in.readLong();
            StreamWindowBinding.TriggerKind trigger =
                    StreamWindowBinding.TriggerKind.fromWireId(in.readInt());
            long triggerInterval = in.readLong();
            boolean purging = in.readBoolean();
            StreamWindowBinding.AccumulationMode accumulation =
                    StreamWindowBinding.AccumulationMode.fromWireId(
                            in.readInt());
            long allowedLateness = in.readLong();
            StreamWindowBinding.FunctionKind functionKind =
                    StreamWindowBinding.FunctionKind.fromWireId(in.readInt());
            String deploymentSetId = readText(in, 64,
                    "window.deploymentSetId");
            String deploymentId = readText(in, 256, "window.deploymentId");
            String jarDigest = readText(in, 64,
                    "window.deploymentJarDigest");
            String className = readText(in, 4096, "window.className");
            FunctionAbi abi = FunctionAbi.fromWireId(in.readInt());
            String codecId = readText(in, 256,
                    "window.configurationCodecId");
            String configurationDigest = readText(in, 64,
                    "window.configurationDigest");
            byte[] configuration = readFrame(in, "window.configuration");
            FunctionDeploymentReference deployment =
                    new FunctionDeploymentReference(deploymentSetId,
                            deploymentId, jarDigest, className, abi, codecId,
                            configurationDigest, configuration,
                            new FunctionResourceProfile(in.readLong(),
                                    in.readLong(), in.readInt()));
            StreamWindowResourceDescriptor resources =
                    new StreamWindowResourceDescriptor(in.readInt(),
                            in.readLong(), in.readLong(), in.readLong(),
                            in.readInt(), in.readInt(), in.readLong());
            StreamOutputTagDescriptor lateOutput = null;
            if (tagged && in.readBoolean()) {
                lateOutput = new StreamOutputTagDescriptor(
                        readText(in, 256, "window.lateOutput.id"),
                        readSchema(in),
                        StreamOutputTagDescriptor.ChangelogMode.fromWireId(
                                in.readInt()),
                        readTexts(in, 0, 1024, "window.lateOutput.keys"),
                        in.readInt(), in.readLong(), in.readInt(),
                        in.readLong(), in.readInt(), in.readLong());
            }
            return new StreamWindowBinding(assigner, eventTimeField,
                    sizeOrGap, slide, trigger, triggerInterval, purging,
                    accumulation, allowedLateness, functionKind, deployment,
                    resources, lateOutput);
        }
        if (kind == 8) {
            StreamAsyncBinding.Ordering ordering =
                    StreamAsyncBinding.Ordering.fromWireId(in.readInt());
            StreamAsyncBinding.ReplayMode replayMode =
                    StreamAsyncBinding.ReplayMode.fromWireId(in.readInt());
            String authorityId = null;
            String authorityDigest = null;
            if (in.readBoolean()) {
                authorityId = readText(in, 256, "async.authorityId");
                authorityDigest = readText(in, 64,
                        "async.authorityDigest");
            }
            String deploymentSetId = readText(in, 64,
                    "async.deploymentSetId");
            String deploymentId = readText(in, 256, "async.deploymentId");
            String jarDigest = readText(in, 64,
                    "async.deploymentJarDigest");
            String className = readText(in, 4096, "async.className");
            FunctionAbi abi = FunctionAbi.fromWireId(in.readInt());
            String codecId = readText(in, 256,
                    "async.configurationCodecId");
            String configurationDigest = readText(in, 64,
                    "async.configurationDigest");
            byte[] configuration = readFrame(in, "async.configuration");
            FunctionDeploymentReference deployment =
                    new FunctionDeploymentReference(deploymentSetId,
                            deploymentId, jarDigest, className, abi, codecId,
                            configurationDigest, configuration,
                            new FunctionResourceProfile(in.readLong(),
                                    in.readLong(), in.readInt()));
            StreamAsyncResourceDescriptor resources =
                    new StreamAsyncResourceDescriptor(in.readInt(),
                            in.readLong(), in.readLong(), in.readInt(),
                            in.readLong(), in.readLong(), in.readInt(),
                            in.readLong());
            return new StreamAsyncBinding(deployment, ordering, replayMode,
                    authorityId, authorityDigest, resources);
        }
        throw fail("BINDING_KIND_MISMATCH", "nodes.binding",
                "unknown binding kind");
    }
    private static StreamStateDescriptor stateByName(
            List<StreamStateDescriptor> states, String name) {
        for (StreamStateDescriptor state : states) {
            if (state.stateName().equals(name)) return state;
        }
        throw fail("STATE_BINDING_MISMATCH", "process.managedStates",
                "process state resource references an undeclared state");
    }
    private static PortableRowSchema readSchema(DataInputStream in)throws IOException{int count=readCount(in,1,PortableRowSchema.MAX_FIELDS,"schema.fields");List<PortableField> fields=new ArrayList<>(count);for(int i=0;i<count;i++)fields.add(new PortableField(readText(in,PortableField.MAX_FIELD_ID_BYTES,"field.id"),readText(in,PortableField.MAX_NAME_BYTES,"field.name"),readText(in,PortableField.MAX_NAME_BYTES,"field.alias"),PortableDataType.fromWireId(in.readInt()),in.readBoolean()));return new PortableRowSchema(fields);}
    private static List<String> readTexts(DataInputStream in,int min,int max,String path)throws IOException{int count=readCount(in,min,max,path);List<String> out=new ArrayList<>(count);for(int i=0;i<count;i++)out.add(readText(in,StreamContractSupport.MAX_TEXT_BYTES,path));return out;}private static int readCount(DataInputStream in,int min,int max,String path)throws IOException{int count=in.readInt();if(count<min||count>max||count>(in.available()/4)+1)throw fail("LIMIT_EXCEEDED",path,"count exceeds limit");return count;}private static String readText(DataInputStream in,int max,String path)throws IOException{byte[] b=readBytes(in,max,path);try{return StreamContractSupport.text(StandardCharsets.UTF_8.newDecoder().onMalformedInput(java.nio.charset.CodingErrorAction.REPORT).onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT).decode(java.nio.ByteBuffer.wrap(b)).toString(),path,max);}catch(java.nio.charset.CharacterCodingException malformed){throw fail("MALFORMED_UTF8",path,"text is not valid UTF-8");}}private static byte[] readFrame(DataInputStream in,String path)throws IOException{return readBytes(in,StreamContractSupport.MAX_FRAME_BYTES,path);}private static byte[] readBytes(DataInputStream in,int max,String path)throws IOException{int n=in.readInt();if(n<0||n>max||n>in.available())throw fail("LIMIT_EXCEEDED",path,"length exceeds limit");byte[] b=new byte[n];in.readFully(b);return b;}
    private static StreamContractException fail(String c,String p,String m){return StreamContractSupport.failure(c,p,m);}private static byte[] sha256(byte[] b){try{return MessageDigest.getInstance("SHA-256").digest(b);}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}private static byte[] sha256(byte[] b,int o,int n){try{MessageDigest d=MessageDigest.getInstance("SHA-256");d.update(b,o,n);return d.digest();}catch(NoSuchAlgorithmException e){throw new IllegalStateException(e);}}
}
