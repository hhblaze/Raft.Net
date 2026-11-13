extern alias baseline;

using System.Reflection;
using System.Reflection.Emit;

namespace Raft.Tests;

public sealed class ArchitectureTests
{
    [Fact]
    public void Original_public_signatures_and_enum_values_remain_available()
    {
        var original = Surface(typeof(baseline::Raft.RaftNode).Assembly);
        var current = Surface(typeof(RaftNode).Assembly);
        var missing = original.Except(current).ToArray();
        Assert.True(missing.Length == 0, string.Join(Environment.NewLine, missing));
        Assert.True(original.Count >= 272);
    }

    [Fact]
    public void Every_core_method_and_field_is_free_of_direct_IO_and_runtime_services()
    {
        var opcodes = typeof(OpCodes).GetFields(BindingFlags.Static | BindingFlags.Public)
            .Where(field => field.FieldType == typeof(OpCode)).Select(field => (OpCode)field.GetValue(null)!)
            .GroupBy(opcode => opcode.Value).ToDictionary(group => group.Key, group => group.First());
        var violations = new List<string>();
        var checkedMethods = 0;
        foreach (var type in typeof(RaftNode).Assembly.GetTypes().Where(type => type.Namespace == "Raft.Core"))
        {
            foreach (var field in type.GetFields(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
                Check(field.FieldType, $"{type}.{field.Name}");
            var methods = type.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly).Cast<MethodBase>()
                .Concat(type.GetConstructors(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic));
            foreach (var method in methods)
            {
                foreach (var parameter in method.GetParameters()) Check(parameter.ParameterType, $"{type}.{method.Name} parameter");
                if (method is MethodInfo info) Check(info.ReturnType, $"{type}.{method.Name} return");
                var bytes = method.GetMethodBody()?.GetILAsByteArray();
                if (bytes is null) continue;
                checkedMethods++;
                for (var position = 0; position < bytes.Length;)
                {
                    var code = bytes[position++];
                    var opcode = opcodes[code == 0xfe ? unchecked((short)(0xfe00 | bytes[position++])) : code];
                    if (opcode.OperandType is OperandType.InlineMethod or OperandType.InlineField or OperandType.InlineType)
                    {
                        var member = method.Module.ResolveMember(BitConverter.ToInt32(bytes, position), type.GetGenericArguments(),
                            method.IsGenericMethod ? method.GetGenericArguments() : null)!;
                        Check(member is Type operand ? operand : member.DeclaringType!, $"{type}.{method.Name} -> {member}");
                    }
                    position += opcode.OperandType switch
                    {
                        OperandType.InlineNone => 0,
                        OperandType.ShortInlineI or OperandType.ShortInlineVar or OperandType.ShortInlineBrTarget => 1,
                        OperandType.InlineVar => 2,
                        OperandType.InlineI8 or OperandType.InlineR => 8,
                        OperandType.InlineSwitch => 4 + 4 * BitConverter.ToInt32(bytes, position),
                        _ => 4
                    };
                }
            }
        }
        Assert.True(checkedMethods >= 1000, "The guard must inspect generated records and reducer helpers too.");
        Assert.True(violations.Count == 0, string.Join(Environment.NewLine, violations));
        return;

        void Check(Type type, string location)
        {
            if (Forbidden(type)) violations.Add($"{location}: {type}");
        }
    }

    private static bool Forbidden(Type type)
    {
        if (type.HasElementType) return Forbidden(type.GetElementType()!);
        if (type.IsGenericParameter) return false;
        if (type.IsGenericType && type.GetGenericArguments().Any(Forbidden)) return true;
        var name = type.FullName ?? type.Name;
        return name.StartsWith("System.Threading.", StringComparison.Ordinal) ||
            name.StartsWith("System.Net.", StringComparison.Ordinal) || name.StartsWith("System.IO.", StringComparison.Ordinal) ||
            name.StartsWith("System.Buffers.ArrayPool", StringComparison.Ordinal) ||
            name.StartsWith("Raft.Runtime.", StringComparison.Ordinal) || name.StartsWith("DBreeze.", StringComparison.Ordinal) ||
            (type.IsInterface && type.Namespace == "Raft.Storage") ||
            name is "System.Random" or "System.DateTime" or "System.DateTimeOffset" or "System.Diagnostics.Stopwatch" or
                "Raft.IRandom" or "Raft.IClock" or "Raft.ITicker" or "Raft.ITransport" or "Raft.IRaftTransport" or
                "Raft.IStateMachine" or "Raft.IRaftStateMachine" or "Raft.IBatchStateMachine" or "Raft.IRaftBatchStateMachine" or
                "Raft.Storage.ILogStore" or "Raft.Storage.IRaftStorage" or "Raft.Storage.IExtendedRaftStorage";
    }

    private static HashSet<string> Surface(Assembly assembly)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in assembly.GetExportedTypes())
        {
            var prefix = type.FullName!;
            result.Add($"type {prefix} {type.BaseType?.FullName}");
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Static).Cast<MethodBase>()
                .Concat(type.GetConstructors()))
            {
                var parameters = string.Join(',', method.GetParameters().Select(parameter =>
                    $"{parameter.ParameterType}:{parameter.Name}:{parameter.IsOptional}:{parameter.DefaultValue}"));
                result.Add($"{prefix}.{method.Name}({parameters}):{(method as MethodInfo)?.ReturnType}:{method.IsStatic}");
            }
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Static))
                result.Add($"{prefix}.{field.Name}:{field.FieldType}:{(field.IsLiteral ? field.GetRawConstantValue() : null)}");
        }
        return result;
    }
}
