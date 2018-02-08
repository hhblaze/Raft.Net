using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Raft.Transport.UdpServer
{
    //!!!!!!!!!!!! NOT USED
    public class UdpSocketClient
    {
        System.Net.Sockets.UdpClient client=null;

        public UdpSocketClient(int port)
        {
            client = new System.Net.Sockets.UdpClient(port);// (new IPEndPoint(IPAddress.Parse("192.168.101.38"), 40001));
            //client.SendAsync
            //client.Connect(new IPEndPoint(IPAddress.Parse("192.168.101.38"), 40000));

           
        }

        public async void SendAsync(byte[] data,IPEndPoint endpoint)
        {            
            if (client == null || data == null || data.Length < 1)
                return;

            try
            {
                int x = await client.SendAsync(data, data.Length, endpoint);
                //int x = await client.SendAsync(data, data.Length);
            }
            catch (Exception ex)
            {
                
                throw;
            }
        }
    }
}
