using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text.Json;
using Raft.Core;
using Raft.Storage;

namespace Raft.Simulation;

internal sealed partial class SeededSimulation
{
    private sealed class ModelIncoming(ModelImage image)
    {
        public readonly ModelImage Image = image;
        public readonly byte[] Bytes = new byte[image.Bytes.Length];
        public long Offset;
        public bool Verifying;
    }
    private readonly Dictionary<string, ModelImage> _images = new(StringComparer.Ordinal);

    private void BuildSnapshot(Node node)
    {
        if (node.Building || node.ApplyPaused || node.State.InstallingSnapshot is not null) return;
        var checkpoint = node.Application.Checkpoint;
        if (checkpoint.Index <= (node.State.Snapshot?.LastIncludedIndex ?? 0)) return;
        var configuration = node.Disk.Query(new LogQuery.ConfigurationAt(checkpoint.Index)).Configuration!;
        var prefix = node.Disk.History.Values.Where(entry => entry.Index <= checkpoint.Index).ToImmutableArray();
        var body = JsonSerializer.SerializeToUtf8Bytes(new { Checkpoint = checkpoint,
            Values = node.Application.Values.OrderBy(pair => pair.Key).ToArray(), Ids = node.Application.CommandIds.Order().ToArray() });
        var bytes = new byte[Math.Max(2048, body.Length + 4)]; // multiple bounded chunks in every image
        BinaryPrimitives.WriteInt32LittleEndian(bytes, body.Length);
        body.CopyTo(bytes.AsSpan(4));
        var build = ++node.BuildId;
        var id = $"s_{node.Id}_{node.Epoch}_{build}";
        var metadata = new SnapshotMetadata(checkpoint.Index, checkpoint.Term, configuration.Voters, configuration.Learners,
            id, ImmutableArray.CreateRange(SHA256.HashData(bytes)));
        var image = new ModelImage(metadata, bytes, prefix, node.Application.Values.ToImmutableDictionary(), node.Application.CommandIds.ToImmutableHashSet());
        var client = ++_clientId;
        node.Building = true;
        AtInstance(node, IoDelay() * 3L, "image-publication", () =>
        {
            if (!node.Building || node.BuildId != build || node.State.InstallingSnapshot is not null) return;
            node.Disk.Images[id] = image;
            _images[id] = image;
            Enqueue(node, new(new CoreEvent.SnapshotCreated(client, metadata)));
        });
    }

    private void ReadSnapshot(Node node, CoreEffect.ReadSnapshotChunk effect)
    {
        AtInstance(node, IoDelay(), "snapshot-chunk-read", () =>
        {
            var image = node.Disk.Images[effect.Metadata.StorageId];
            Require(effect.Offset >= 0 && effect.Offset <= image.Bytes.Length, "Snapshot read escaped image.");
            var count = (int)Math.Min(effect.Count, image.Bytes.Length - effect.Offset);
            var data = new SnapshotChunkData(image.Bytes.Length, image.Bytes.AsMemory((int)effect.Offset, count));
            Reduce(node, new CoreEvent.SnapshotChunkRead(effect.Id, data));
        });
    }

    private void StageSnapshot(Node node, RpcContext rpc, SnapshotChunkRequest request)
    {
        AtInstance(node, IoDelay(), "snapshot-data-flush", () =>
        {
            if (node.Disk.Images.ContainsKey(request.StorageId))
            {
                Enqueue(node, new(new CoreEvent.SnapshotReady(rpc.From, request.Term, request.Metadata!)));
                SendReply(node, rpc, new SnapshotChunkResponse(node.State.HardState.Term, request.RequestId, request.StorageId, request.TotalLength, true, false));
                return;
            }
            var incoming = node.Incoming;
            if (incoming is null || incoming.Image.Metadata.StorageId != request.StorageId)
            {
                if (incoming is not null && incoming.Image.Metadata.LastIncludedIndex > request.Metadata!.LastIncludedIndex)
                {
                    SendReply(node, rpc, new SnapshotChunkResponse(node.State.HardState.Term, request.RequestId, request.StorageId, 0, false, false));
                    return;
                }
                incoming = new ModelIncoming(_images[request.StorageId]);
                node.Incoming = incoming;
            }
            var staged = incoming;
            if (request.Offset > staged.Offset)
            {
                SendReply(node, rpc, new SnapshotChunkResponse(node.State.HardState.Term, request.RequestId, request.StorageId, staged.Offset, false, false));
                return;
            }
            if (request.Offset == staged.Offset) request.Data.CopyTo(staged.Bytes.AsMemory((int)request.Offset));
            // A crash may leave fsynced unpromised bytes beyond Offset; progress is a separate durable publication.
            AtInstance(node, IoDelay(), "snapshot-progress-flush", () =>
            {
                if (!ReferenceEquals(staged, node.Incoming))
                {
                    SendReply(node, rpc, new SnapshotChunkResponse(node.State.HardState.Term, request.RequestId, request.StorageId, 0, false, false));
                    return;
                }
                staged.Offset = Math.Max(staged.Offset, request.Offset + request.Data.Length);
                SendReply(node, rpc, new SnapshotChunkResponse(node.State.HardState.Term, request.RequestId, request.StorageId, staged.Offset, true, false));
                if (staged.Offset != staged.Bytes.Length || staged.Verifying) return;
                staged.Verifying = true;
                AtInstance(node, IoDelay() * 3L, "snapshot-verify-prepare", () =>
                {
                    Require(CryptographicOperations.FixedTimeEquals(SHA256.HashData(staged.Bytes), staged.Image.Metadata.Digest.AsSpan()), "Staged image checksum differs.");
                    node.Disk.Images[request.StorageId] = staged.Image;
                    Enqueue(node, new(new CoreEvent.SnapshotReady(rpc.From, request.Term, staged.Image.Metadata)));
                });
            });
        });
    }

    private void ActivateSnapshot(Node node, CoreEffect.ActivateSnapshot effect)
    {
        node.ApplyPaused = true;
        node.Building = false;
        node.BuildId++;
        AtInstance(node, IoDelay() * 2L, "snapshot-generation-switch", Activate);
        return;
        void Activate()
        {
            if (node.Applying) { AtInstance(node, 2, "await-apply-before-install", Activate); return; }
            node.Application.Install(node.Disk.Images[effect.Metadata.StorageId]);
            _installs++;
            CompleteClients(node);
            Enqueue(node, new(new CoreEvent.SnapshotCompleted(effect.Id, effect.Metadata)));
        }
    }
}
