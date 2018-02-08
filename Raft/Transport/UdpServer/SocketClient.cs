using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport
{
    internal class SocketClient : SocketSender
    {
        TcpServer Server = null;
        public Socket Socket = null;
        public byte[] Buffer = new byte[10000];	    //Receive buffer

        /// <summary>
        /// Is given by TcpServer and can'be counted as connection, instance Id
        /// </summary>
        public long InstanceId = 0;

        /// <summary>
        /// Granted by TcpServer to the Client. Must be disposed
        /// </summary>
        public SocketAsyncEventArgs SocketAsyncEventArgs = null;

        /// <summary>
        /// Client of a higher level
        /// </summary>
        public ITcpClient tcpClient = null;

        public SocketClient(Socket socket, TcpServer server)
        {
            Socket = socket;
            Server = server;
        }


        object lock_dispose = new object();
        private bool disposed = false;

        public bool Disposed
        {
            get
            {
                lock (lock_dispose)
                {
                    return this.disposed;
                }
            }
        }

        public override void Dispose()
        {
            lock (lock_dispose)
            {
                if (disposed)
                    return;

                disposed = true;
            }

            base.Dispose();

            try
            {
                if (SocketAsyncEventArgs != null)
                {                    
                    Buffer = null;
                    SocketAsyncEventArgs.Dispose();
                    SocketAsyncEventArgs = null;
                }
            }
            catch
            { }

            //!!!!!!!!!!!!!!!!!  REMARKED
          //  WSN_TcpServer.StaticServices._S.JobManager.RemoveActionById(this.NotHandshakedDisconnectTimerId);

            if (Socket != null)
            {
                try
                {
                    //ensures that all queud for sending data is sent, we don't need it here
                    Socket.Shutdown(SocketShutdown.Both);
                }
                catch (System.Exception ex)
                {
                }


                //http://msdn.microsoft.com/en-us/library/system.net.sockets.socket.receivetimeout.aspx
                //we use disconnect instead of Close http://msdn.microsoft.com/en-us/library/system.net.sockets.socket.disconnect.aspx
                Socket.Close();
                Socket = null;
            }

            //Dsiposing client of higher abstraction level
            try
            {
                if (this.tcpClient != null)
                {
                    this.tcpClient.Dispose();
                }
            }
            catch { }
        }

    }
}
