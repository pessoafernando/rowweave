package io.rowweave.stream.contract;

import io.rowweave.portable.PortableRowSchema;
import java.util.*;

/** Immutable topologically ordered Stream operator descriptor. */
public final class PortableStreamNode {
    private final String operatorUid; private final StreamOperatorCode opcode;
    private final List<String> inputs; private final List<String> inputSchemas;
    private final PortableRowSchema outputSchema; private final List<String> stableKeys;
    private final List<StreamStateDescriptor> states; private final SortedMap<String,String> parameters;
    private final StreamNodeBinding binding;
    private final StreamNodeExecutionDescriptor execution;

    public PortableStreamNode(String operatorUid, StreamOperatorCode opcode, List<String> inputs,
                              List<String> inputSchemas, PortableRowSchema outputSchema,
                              List<String> stableKeys, List<StreamStateDescriptor> states,
                              Map<String,String> parameters, StreamNodeBinding binding) {
        this(operatorUid, opcode, inputs, inputSchemas, outputSchema,
                stableKeys, states, parameters, binding, null);
    }

    public PortableStreamNode(String operatorUid, StreamOperatorCode opcode, List<String> inputs,
                              List<String> inputSchemas, PortableRowSchema outputSchema,
                              List<String> stableKeys, List<StreamStateDescriptor> states,
                              Map<String,String> parameters, StreamNodeBinding binding,
                              StreamNodeExecutionDescriptor execution) {
        this.operatorUid=StreamContractSupport.text(operatorUid,"nodes.operatorUid",256);
        if(opcode==null)throw StreamContractSupport.failure("UNKNOWN_OPCODE","nodes.opcode","opcode is required");
        this.opcode=opcode;this.inputs=texts(inputs,opcode.inputCount(),"nodes.inputs");
        this.inputSchemas=digests(inputSchemas,opcode.inputCount());
        if(outputSchema==null)throw StreamContractSupport.failure("INVALID_SCHEMA","nodes.outputSchema","schema is required");
        this.outputSchema=outputSchema;this.stableKeys=texts(stableKeys,-1,"nodes.stableKeys");
        if(states==null||states.size()>64)throw StreamContractSupport.failure("LIMIT_EXCEEDED","nodes.states","state count exceeds limit");
        Set<String> names=new HashSet<>();List<StreamStateDescriptor> stateCopy=new ArrayList<>();
        for(StreamStateDescriptor state:states){if(state==null||!names.add(state.stateName()))throw StreamContractSupport.failure("DUPLICATE_STATE","nodes.states","state names must be unique");stateCopy.add(state);}
        this.states=Collections.unmodifiableList(stateCopy);
        if(parameters==null||parameters.size()>64)throw StreamContractSupport.failure("LIMIT_EXCEEDED","nodes.parameters","parameter count exceeds limit");
        SortedMap<String,String> p=new TreeMap<>();for(Map.Entry<String,String> e:parameters.entrySet())p.put(StreamContractSupport.text(e.getKey(),"nodes.parameter.key",256),StreamContractSupport.text(e.getValue(),"nodes.parameter.value",4096));this.parameters=Collections.unmodifiableSortedMap(p);
        if(opcode.bindingKind()==StreamOperatorCode.BindingKind.NONE){if(binding!=null)bindingError();}
        else if(binding==null||binding.kind()!=opcode.bindingKind())bindingError();
        if(binding instanceof StreamFunctionBinding
                && !((StreamFunctionBinding) binding).matches(opcode)) {
            throw StreamContractSupport.failure("FUNCTION_KIND_MISMATCH",
                    "nodes.function.kind", "function kind does not match opcode");
        }
        if (binding instanceof StreamProcessBinding
                && !((StreamProcessBinding) binding).matches(opcode)) {
            throw StreamContractSupport.failure("FUNCTION_KIND_MISMATCH",
                    "nodes.process.kind",
                    "process function kind does not match opcode");
        }
        if ((opcode == StreamOperatorCode.KEYED_PROCESS
                || opcode == StreamOperatorCode.KEYED_CO_PROCESS)
                && !(binding instanceof StreamProcessBinding)) {
            bindingError();
        }
        if (binding instanceof StreamCalcBinding) {
            ((StreamCalcBinding) binding).validateOutputSchema(outputSchema);
        } else if (opcode.bindingKind()
                == StreamOperatorCode.BindingKind.CALC) {
            bindingError();
        }
        if (opcode.bindingKind() == StreamOperatorCode.BindingKind.TIME
                && !(binding instanceof StreamTimeBinding)) {
            bindingError();
        }
        this.binding=binding;
        this.execution=execution;
    }
    private static void bindingError(){throw StreamContractSupport.failure("BINDING_KIND_MISMATCH","nodes.binding","binding does not match opcode");}
    private static List<String> texts(List<String> values,int exact,String path){if(values==null||(exact>=0&&values.size()!=exact)||values.size()>1024)throw StreamContractSupport.failure("INVALID_TOPOLOGY",path,"invalid list size");List<String> out=new ArrayList<>();for(String v:values)out.add(StreamContractSupport.text(v,path,256));return Collections.unmodifiableList(out);}
    private static List<String> digests(List<String> values,int exact){if(values==null||values.size()!=exact)throw StreamContractSupport.failure("INVALID_TOPOLOGY","nodes.inputSchemas","invalid input schema count");List<String> out=new ArrayList<>();for(String v:values)out.add(StreamContractSupport.digest(v,"nodes.inputSchemas"));return Collections.unmodifiableList(out);}
    public String operatorUid(){return operatorUid;} public StreamOperatorCode opcode(){return opcode;} public List<String> inputOperatorUids(){return inputs;}
    public List<String> inputSchemaFingerprints(){return inputSchemas;} public PortableRowSchema outputSchema(){return outputSchema;}
    public List<String> stableKeyFields(){return stableKeys;} public List<StreamStateDescriptor> stateDescriptors(){return states;}
    public SortedMap<String,String> scalarParameters(){return parameters;} public StreamNodeBinding binding(){return binding;}
    public StreamNodeExecutionDescriptor executionDescriptor(){return execution;}
    @Override public boolean equals(Object o){if(!(o instanceof PortableStreamNode))return false;PortableStreamNode n=(PortableStreamNode)o;return operatorUid.equals(n.operatorUid)&&opcode==n.opcode&&inputs.equals(n.inputs)&&inputSchemas.equals(n.inputSchemas)&&outputSchema.equals(n.outputSchema)&&stableKeys.equals(n.stableKeys)&&states.equals(n.states)&&parameters.equals(n.parameters)&&Objects.equals(binding,n.binding)&&Objects.equals(execution,n.execution);}
    @Override public int hashCode(){return Objects.hash(operatorUid,opcode,inputs,inputSchemas,outputSchema,stableKeys,states,parameters,binding,execution);}
}
