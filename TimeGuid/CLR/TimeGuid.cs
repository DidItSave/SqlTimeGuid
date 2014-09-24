using Microsoft.SqlServer.Server;
using System;
using System.Data.SqlTypes;

public partial class UserDefinedFunctions
{
    /// <summary>
    /// Helper function to generate a SqlGuid using the supplied timestamp bytes and
    /// random guid bytes
    /// </summary>
    /// <param name="timestamp">Time stamp portion of the guid</param>
    /// <param name="guidBytes">The random portion of the guid</param>
    /// <returns>SqlGuid that combines the supplied timestamp and guidbytes</returns>
    private static SqlGuid GenerateSqlGuid(byte[] timestamp, byte[] guidBytes)
    {
        // Reverse the bytes if LittleEndian
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(timestamp);
        }

        var newGuidBytes = new byte[16];

        Buffer.BlockCopy(guidBytes, 0, newGuidBytes, 0, 10);
        Buffer.BlockCopy(timestamp, 2, newGuidBytes, 10, 6);

        return new SqlGuid(newGuidBytes);
    }

    /// <summary>
    /// Parse the supplied dateTime string and default to UTC now if needed
    /// </summary>
    /// <param name="dateTime">Supplied date and time in string format</param>
    /// <returns>DateTime value</returns>
    private static DateTime GetDateTime(string dateTime)
    {
        DateTime dt;

        if (!DateTime.TryParse(dateTime, out dt))
        {
            dt = DateTime.UtcNow;
        }

        return dt;
    }

    /// <summary>
    /// Generates a new SqlGuid (Guid) that is time based.  This allows for semi-sequential
    /// guids to be generated within SQL Server.  They are semi-sequential as there is still
    /// a random element to them.  The randomness is limited to the millisecond they are
    /// generated in.
    /// </summary>
    /// <param name="dateTime">Date and time (UTC) the Guid applies to. Supplied as a string to allow null values in SQL Server</param>
    /// <returns>A new time based Guid</returns>
    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None)]
    public static SqlGuid NewTimeGuid(String dateTime)
    {
        long timestamp = GetDateTime(dateTime).Ticks / 10000L;
        var timestampBytes = BitConverter.GetBytes(timestamp);

        var random = new System.Security.Cryptography.RNGCryptoServiceProvider();
        var randomBytes = new byte[10];
        random.GetBytes(randomBytes);

        return GenerateSqlGuid(timestampBytes, randomBytes);
    }

    /// <summary>
    /// Returns the maximum Guid value for a specified date time.  This can be used with SQL
    /// statements to perform range scans.
    /// </summary>
    /// <param name="dateTime">Date and time (UTC) the max guid value should be generated for.  If null uses the current UTC value</param>
    /// <returns>Guid that is the max possible value for the specified time period</returns>
    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, IsDeterministic = true)]
    public static SqlGuid MaxTimeGuid(String dateTime)
    {
        long timestamp = GetDateTime(dateTime).Ticks / 10000L;
        var timestampBytes = BitConverter.GetBytes(timestamp);

        var fillBytes = new Byte[10];

        for (int i = 0; i < fillBytes.Length; i++)
        {
            fillBytes[i] = 0xFF;
        }

        return GenerateSqlGuid(timestampBytes, fillBytes);
    }

    /// <summary>
    /// Returns the minimum Guid value for a specified date time.  This can be used with SQL
    /// statements to perform range scans.
    /// </summary>
    /// <param name="dateTime">Date and time (UTC) the min guid value should be generated for.  If null uses the current UTC value</param>
    /// <returns>Guid that is the min possible value for the specified time period</returns>
    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None, IsDeterministic = true)]
    public static SqlGuid MinTimeGuid(String dateTime)
    {
        long timestamp = GetDateTime(dateTime).Ticks / 10000L;
        var timestampBytes = BitConverter.GetBytes(timestamp);

        var fillBytes = new Byte[10];

        for (int i = 0; i < fillBytes.Length; i++)
        {
            fillBytes[i] = 0x00;
        }

        return GenerateSqlGuid(timestampBytes, fillBytes);
    }

    /// <summary>
    /// Returns the Date and Time portion (UTC) of the GUID supplied
    /// </summary>
    /// <param name="guid">The guid to pull the date and time from</param>
    /// <returns>The date and time encoded in the guid</returns>
    [Microsoft.SqlServer.Server.SqlFunction(DataAccess = DataAccessKind.None)]
    public static DateTime TimeGuidDateTime(SqlGuid guid)
    {
        var bytes = guid.ToByteArray();
        var timestampBytes = new byte[8];

        Buffer.BlockCopy(bytes, 10, timestampBytes, 2, 6);

        // Reverse the bytes if LittleEndian
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(timestampBytes);
        }

        DateTime guidDate = new DateTime(BitConverter.ToInt64(timestampBytes, 0) * 10000);

        return guidDate;
    }
}