using System;
using System.Text;

namespace BookSleeve
{
    public sealed class RedisFeatures
    {
        private readonly Version version;
        public Version Version { get { return version; } }
        public RedisFeatures(Version version)
        {
            if (version == null) throw new ArgumentNullException("version");
            this.version = version;
        }

        private static readonly Version v2_1_2 = new Version("2.1.2"), v2_1_3 = new Version("2.1.3");
        public bool Persist { get { return version >= v2_1_2; } }
        public bool ExpireOverwrite { get { return version >= v2_1_3; } }
        public override string ToString()
        {
            var sb = new StringBuilder().Append("Features in ").Append(version).AppendLine()
                .Append("ExpireOverwrite: ").Append(ExpireOverwrite).AppendLine()
                .Append("Persist: ").Append(Persist).AppendLine();

            return sb.ToString();
        }
    }
}
