using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSleeve
{
    internal abstract class MessageResult
    {
        public abstract void Complete(RedisResult result);
    }
    internal sealed class MessageResultDouble : MessageResult
    {
        private readonly TaskCompletionSource<double> source = new TaskCompletionSource<double>();
        public Task<double> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                double value;
                try
                {
                    value = result.ValueDouble;
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }
    internal sealed class MessageResultInt64 : MessageResult
    {
        private readonly TaskCompletionSource<long> source = new TaskCompletionSource<long>();
        public Task<long> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                long value;
                try
                {
                    value = result.ValueInt64;
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }
    internal sealed class MessageResultBoolean : MessageResult
    {
        private readonly TaskCompletionSource<bool> source = new TaskCompletionSource<bool>();
        public Task<bool> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                bool value;
                try
                {
                    value = result.ValueBoolean;
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }

    internal sealed class MessageResultString : MessageResult
    {
        private readonly TaskCompletionSource<string> source = new TaskCompletionSource<string>();
        public Task<string> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                string value;
                try
                {
                    value = result.ValueString;
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }

    internal sealed class MessageResultMultiString : MessageResult
    {
        private readonly TaskCompletionSource<string[]> source = new TaskCompletionSource<string[]>();
        public Task<string[]> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                string[] value;
                try
                {
                    value = result.ValueItemsString();
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }

    internal sealed class MessageResultBytes : MessageResult
    {
        private readonly TaskCompletionSource<byte[]> source = new TaskCompletionSource<byte[]>();
        public Task<byte[]> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                byte[] value;
                try
                {
                    value = result.ValueBytes;
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }

    internal sealed class MessageResultMultiBytes : MessageResult
    {
        private readonly TaskCompletionSource<byte[][]> source = new TaskCompletionSource<byte[][]>();
        public Task<byte[][]> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                byte[][] value;
                try
                {
                    value = result.ValueItemsBytes();
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }

    internal sealed class MessageResultPairs : MessageResult
    {
        private readonly TaskCompletionSource<KeyValuePair<byte[], double>[]> source = new TaskCompletionSource<KeyValuePair<byte[], double>[]>();
        public Task<KeyValuePair<byte[], double>[]> Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                KeyValuePair<byte[], double>[] value;
                try
                {
                    value = result.ExtractPairs();
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(value);
            }
        }
    }
    internal sealed class MessageResultVoid : MessageResult
    {
        private readonly TaskCompletionSource<bool> source = new TaskCompletionSource<bool>();
        public Task Task { get { return source.Task; } }
        public override void Complete(RedisResult result)
        {
            if (result.IsError)
            {
                source.SetException(result.Error());
            }
            else
            {
                try
                {
                    result.Assert();
                }
                catch (Exception ex)
                {
                    source.SetException(ex);
                    return;
                }
                source.SetResult(true);
            }
        }
    }


    
}
